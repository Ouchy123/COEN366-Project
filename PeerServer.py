import socket
import threading
import json
import os
import zlib
import time

SERVER_IP = "127.0.0.1"
SERVER_PORT = 5000
DB_FILE = "registered_peers.json"

lock = threading.Lock()

# Load persistent peer data
if os.path.exists(DB_FILE):
    with open(DB_FILE, "r") as f:
        peers = json.load(f)
else:
    peers = {}

heartbeat_table = {}  # {name: last_timestamp}
# backup_table = {
#   (owner, filename): {
#       "file_size": int,
#       "chunk_size": int,
#       "chunks": [
#           { "chunk_id": int, "peer": "PeerName", "store_ack": bool }
#       ]
#   }
# }
backup_table = {}  

#saving to json database
def save_db():
    #Save the current peer dictionary to disk
    try:
        with open(DB_FILE, "w") as f:
            json.dump(peers, f, indent=4)
    except Exception as e:
        print(f"[SERVER] Error saving database: {e}")

def initServer():
    with lock:
        global peers
        peerRemoveList = []
        for peer in peers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.settimeout(3.0)
                    pingMsg = "PING"
                    sock.sendto(pingMsg.encode(), (peers[peer]["IP"], int(peers[peer]["UDP_Port"])))
                    data, _ = sock.recvfrom(1024)
                    response = data.decode().strip()
                    if response == "PONG":
                        print(f"[SERVER] Peer {peer} is online.")
            except socket.timeout:
                print(f"[SERVER] Peer {peer} did not respond on time, therefore we remove it from the list of active peers.")
                peerRemoveList.append(peer)
                
        for peer in peerRemoveList:
            del peers[peer]
    save_db()

def handleRegistration(msg, addr, sock):
    
    print("\n[SERVER] : At handleRegistration\n")
    if len(msg) != 8:
        reply = f"REGISTER-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
        sock.sendto(reply.encode(), addr)
        return
    
    elif msg[3].lower() not in ["owner", "storage", "both"]:
        reply = f"REGISTER-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Role"
        sock.sendto(reply.encode(), addr)
        return
    
    _, rq, name, role, ip, udp_port, tcp_port, storage = msg

    with lock:
        global peers
        if name in peers:
            # Do NOT overwrite; just deny
            reply = f"REGISTER-DENIED {rq} NameAlreadyInUse"
        else:
            peers[name] = {
                "Role": role,
                "IP": ip,
                "UDP_Port": udp_port,
                "TCP_Port": tcp_port,
                "Storage": storage,
            }
            save_db()
            reply = f"REGISTERED {rq}"

    print(f"[SERVER] {name} -> {reply}")
    sock.sendto(reply.encode(), addr)

def handleDeregistration(msg, addr, sock):
    print("\n[SERVER] : At handleDeregistration\n")
    if len(msg) != 3:
        reply = f"DE-REGISTER-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
        sock.sendto(reply.encode(), addr)
        return

    _, rq, name = msg

    with lock:
        if name in peers:
            del peers[name]
            save_db()
            reply = f"DE-REGISTERED {rq}"
            print(f"[SERVER] : {name} deregistered.")
        else:
            reply = f"DE-REGISTER-DENIED {rq} Unknown_Peer"
            print(f"[SERVER] : Unknown peer {name} tried to deregister.")

    sock.sendto(reply.encode(), addr)

def handleBackupRequest(msg, addr, sock):
    print("[SERVER] : Handling Backup-Request: ", msg)

    # Expected: BACKUP_REQ Owner FileName Size Checksum
    if len(msg) != 5:
        reply = f"BACKUP-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
        sock.sendto(reply.encode(), addr)
        return

    # CRC validation
    msgWithoutCRC = ' '.join(msg[:-1])
    calculated_crc = zlib.crc32(msgWithoutCRC.encode()) & 0xFFFFFFFF
    if str(calculated_crc) != msg[-1]:
        print("[SERVER] CRC32 Check FAILED")
        reply = f"BACKUP-DENIED 0 CRC_MISMATCH"
        sock.sendto(reply.encode(), addr)
        return
    else:
        print("[SERVER] CRC32 Check Passed")

    _, owner, filename, size, received_crc = msg
    size_int = int(size)
    print(f"[SERVER] Backup request from {owner} for file {filename} of size {size_int} bytes")

    global backup_table, peers

    with lock:
        key = (owner, filename)
        if key in backup_table:
            print("[SERVER] File already exists in backup table, cannot double backup.")
            msg_denied = f"BACKUP-DENIED RQ {filename} File_Already_Backed_Up"
            sock.sendto(msg_denied.encode(), addr)
            return

        # Choose storage peers: all with Role in [storage, both] and not the owner
        storage_peers = []
        for peer_name, peer_info in peers.items():
            print("[SERVER] - Checking peer:", peer_name,
                  ". Role is:", peer_info["Role"], "and storage is:", peer_info["Storage"])
            if peer_name != owner and peer_info["Role"].lower() in ["storage", "both"]:
                storage_peers.append(peer_name)

        if not storage_peers:
            print("[SERVER] No eligible storage peers.")
            msg_denied = f"BACKUP-DENIED RQ No_Storage_Peer"
            sock.sendto(msg_denied.encode(), addr)
            return

        # Compute chunk size
        chunk_size = max(1, size_int // len(storage_peers))

        # Fill backup_table
        chunks_meta = []
        peer_list_str = ""

        for chunk_id, peer_name in enumerate(storage_peers):
            peer_info = peers[peer_name]
            chunks_meta.append({
                "chunk_id": chunk_id,
                "peer": peer_name,
                "store_ack": False
            })

            # Build peer_list for BACKUP_PLAN
            peer_list_str += f"{peer_name},{peer_info['IP']},{peer_info['TCP_Port']}|"

            # Inform storage peer about upcoming storage (STORAGE_TASK)
            storage_addr = (peer_info["IP"], int(peer_info["UDP_Port"]))
            storage_msg = f"STORAGE_TASK RQ {filename} {chunk_size} {owner}"
            sock.sendto(storage_msg.encode(), storage_addr)
            print(f"[SERVER] Sent STORAGE_TASK to {peer_name}: {storage_msg}")

        backup_table[key] = {
            "file_size": size_int,
            "chunk_size": chunk_size,
            "chunks": chunks_meta
        }

    replyRequester = f"BACKUP_PLAN RQ {filename} {peer_list_str} {chunk_size}"
    sock.sendto(replyRequester.encode(), addr)
    print(f"[SERVER] Sent backup plan to requester {owner}: {replyRequester}")



#function for handling messages
def handle_message(data, addr, sock):
    msg = data.decode().strip().split()
    if not msg:
        return

    cmd = msg[0].upper()
    print("At handle_message and cmd received is:", cmd)

    # Handle registration
    match cmd:
        case "REGISTER":
            print("At REGISTER case")
            handleRegistration(msg,addr,sock)

        case "DE-REGISTER":
            print("At DE-REGISTER case")
            handleDeregistration(msg, addr, sock)

        case "BACKUP_REQ":
            handleBackupRequest(msg, addr, sock)

        case "HEARTBEAT":
            handleHeartbeat(msg, addr, sock)

        case "RESTORE_REQ":
            handleRestoreRequest(msg, addr, sock)

        case "STORE_ACK":
            handleStoreAck(msg, addr, sock)

        case "BACKUP_DONE":
            handleBackupDone(msg, addr, sock)

        case _:
            print(f"[SERVER] Unknown command from {addr}: {data.decode().strip()}")
            sock.sendto(f"ERROR Unknown_Command".encode(), addr)

        
def handleHeartbeat(msg, addr, sock):
    print("[SERVER] Received HEARTBEAT:", msg)

    if len(msg) != 5:
        print("[SERVER] HEARTBEAT: Invalid format")
        return

    _, rq, name, chunk_count, timestamp = msg

    with lock:
        heartbeat_table[name] = int(timestamp)

def handle_peer_failure(peer_name):
    # Skeleton logic: find chunks stored on failed peer and choose a new target
    global backup_table, peers
    print(f"[SERVER] Handling failure of peer {peer_name} for replication (skeleton).")

    with lock:
        for (owner, filename), entry in backup_table.items():
            for chunk in entry["chunks"]:
                if chunk["peer"] == peer_name:
                    # Pick a new target peer
                    target = None
                    for candidate, info in peers.items():
                        if candidate != peer_name and info["Role"].lower() in ["storage", "both"]:
                            target = candidate
                            break

                    if target:
                        print(f"[SERVER] Would replicate chunk {chunk['chunk_id']} of {owner}:{filename} "
                              f"from {peer_name} to {target}")
                        target_info = peers[target]
                        replicate_msg = f"REPLICATE_REQ RQ {owner} {filename} {chunk['chunk_id']} {target}"
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as tmp:
                            tmp.sendto(replicate_msg.encode(),
                                       (target_info["IP"], int(target_info["UDP_Port"])))
                    else:
                        print(f"[SERVER] No available target peer to replicate chunk {chunk['chunk_id']} "
                              f"of {owner}:{filename}")


import time 
def heartbeat_watchdog():
    while True:
        now = int(time.time())

        with lock:
            for peer, last in list(heartbeat_table.items()):
                if now - last > 15:   # more than 15 seconds without heartbeat
                    print(f"[SERVER] Peer {peer} FAILED (No heartbeat detected).")
                    # Later: trigger recovery logic from Section 2.5
                    del heartbeat_table[peer]

        time.sleep(5)

def handleRestoreRequest(msg, addr, sock):

    # Expected: RESTORE_REQ RQ# File_Name
    if len(msg) != 3:
        sock.sendto("RESTORE-DENIED 0 Invalid_Format".encode(), addr)
        return

    _, rq, filename = msg

    global backup_table, peers

    with lock:
        # Find any (owner, filename) pair that matches
        matching_keys = [key for key in backup_table.keys() if key[1] == filename]

        if not matching_keys:
            reply = f"RESTORE-DENIED {rq} File_Not_Found"
            sock.sendto(reply.encode(), addr)
            return

        owner, filename = matching_keys[0]
        entry = backup_table[(owner, filename)]

        peer_list_str = ""
        for chunk in entry["chunks"]:
            peer_name = chunk["peer"]
            if peer_name in peers:
                info = peers[peer_name]
                peer_list_str += f"{peer_name},{info['IP']},{info['TCP_Port']}|"

        reply = f"RESTORE_PLAN {rq} {filename} {peer_list_str}"
        print("[SERVER] Sending RESTORE_PLAN:", reply)

    sock.sendto(reply.encode(), addr)

def handleStoreAck(msg, addr, sock):
    # Expected: STORE_ACK RQ Owner FileName Chunk_ID
    print("[SERVER] Received STORE_ACK:", msg)
    if len(msg) != 5:
        print("[SERVER] STORE_ACK: Invalid format")
        return

    _, rq, owner, filename, chunk_id = msg
    key = (owner, filename)
    cid = int(chunk_id)

    global backup_table
    with lock:
        if key not in backup_table:
            print(f"[SERVER] STORE_ACK: Unknown backup for {owner}:{filename}")
            return

        entry = backup_table[key]
        for chunk in entry["chunks"]:
            if chunk["chunk_id"] == cid:
                chunk["store_ack"] = True
                print(f"[SERVER] STORE_ACK: Marked chunk {cid} of {owner}:{filename} as stored.")
                break


def handleBackupDone(msg, addr, sock):
    # Expected: BACKUP_DONE RQ Owner FileName
    print("[SERVER] Received BACKUP_DONE:", msg)
    if len(msg) != 4:
        print("[SERVER] BACKUP_DONE: Invalid format")
        return

    _, rq, owner, filename = msg
    key = (owner, filename)

    global backup_table
    with lock:
        if key not in backup_table:
            print(f"[SERVER] BACKUP_DONE: Unknown backup for {owner}:{filename}")
            return

        entry = backup_table[key]
        all_ack = all(chunk["store_ack"] for chunk in entry["chunks"])

        if all_ack:
            print(f"[SERVER] Backup COMPLETE for {owner}:{filename} (all chunks have STORE_ACK).")
        else:
            print(f"[SERVER] BACKUP_DONE received but some chunks missing STORE_ACK for {owner}:{filename}.")

def server_thread():
    #Main UDP server loop
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    print(f"[SERVER] Running on {SERVER_IP}:{SERVER_PORT}")

    # Start heartbeat monitoring
    threading.Thread(target=heartbeat_watchdog, daemon=True).start()
    initServer()
    try:
        while True:
            data, addr = sock.recvfrom(1024)
            threading.Thread(target=handle_message, args=(data, addr, sock)).start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down")
        sock.close()
        save_db()
        print("[SERVER] Database saved. Goodbye!")


if __name__ == "__main__":
    server_thread()
