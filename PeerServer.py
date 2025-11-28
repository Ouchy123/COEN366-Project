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
backup_table = {}  # {filename: [peer_list]}

#saving to json database
def save_db():
    #Save the current peer dictionary to disk
    try:
        with open(DB_FILE, "w") as f:
            json.dump(peers, f, indent=4)
    except Exception as e:
        print(f"[SERVER] Error saving database: {e}")

def handleRegistration(msg,addr,sock):
    
    print("At handleRegistration")
    if len(msg) != 8:
        reply = f"REGISTER-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
        sock.sendto(reply.encode(), addr)
        return
    
    elif msg[3].lower() not in ["owner", "storage", "both"]:
        reply = f"REGISTER-DENIED : Invalid_Role"
        sock.sendto(reply.encode(), addr)
        return
    
    
    _, rq, name, role, ip, udp_port, tcp_port, storage = msg

    with lock:
       
        global peers
        if name in peers:
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
    print("At handleDeregistration")
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
            print(f"[SERVER] {name} deregistered.")
        else:
            reply = f"DE-REGISTER-DENIED {rq} Unknown_Peer"
            print(f"[SERVER] Unknown peer {name} tried to deregister.")

    sock.sendto(reply.encode(), addr)

def handleBackupRequest(msg, addr, sock):
    print("[SERVER] Handling Backup-Request: ", msg)

    if len(msg) != 5:
        reply = f"BACKUP-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
        sock.sendto(reply.encode(), addr)
        return

    msgWithoutCRC = ' '.join(msg[:-1])
    calculated_crc = zlib.crc32(msgWithoutCRC.encode()) & 0xFFFFFFFF
    if str(calculated_crc) == msg[-1]:
        print("[SERVER] CRC32 Check Passed")

    _, owner, filename, size, received_crc = msg

    # Select all storage peers (owner or storage/both)
    with lock:
        if filename not in backup_table:
            backup_table[filename] = []

        for peer_name, info in peers.items():
            # simplistic assignment — real allocation can be smarter
            entry = {
                "peer": peer_name,
                "owner": owner,
                "ip": info["IP"],
                "tcp": info["TCP_Port"]
            }
            backup_table[filename].append(entry)

    print("[SERVER] Updated backup table:", backup_table)

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

    # Handle de-registration
        case "DE-REGISTER":
            print("At DE-REGISTER case")
            handleDeregistration(msg, addr, sock)

        case "BACKUP-REQUEST":
            handleBackupRequest(msg, addr, sock)

    # Handle heartbeat
        case "HEARTBEAT":
            handleHeartbeat(msg, addr, sock)

        case "RESTORE_REQ":
            handleRestoreRequest(msg, addr, sock)
        case "REPLICATE_DONE":
            handleReplicateDone(msg, addr, sock)
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

import time 
def heartbeat_watchdog(sock):
    while True:
        now = int(time.time())

        with lock:
            for peer, last in list(heartbeat_table.items()):
                if now - last > 15:
                    print(f"[SERVER] Peer {peer} FAILED (No heartbeat detected).")

                    del heartbeat_table[peer]

                    handle_peer_failure(peer, sock)

        time.sleep(5)


def handle_peer_failure(dead_peer, sock):
    print(f"[SERVER] Handling failure of {dead_peer}...")

    # For each file that used the dead peer
    for filename, entry_list in backup_table.items():

        # Find entries belonging to dead peer
        lost_entries = [e for e in entry_list if e["peer"] == dead_peer]
        if not lost_entries:
            continue

        print(f"[SERVER] Peer {dead_peer} had a replica of {filename}")

        # Remove failed peer from backup list
        backup_table[filename] = [e for e in entry_list if e["peer"] != dead_peer]

        # Pick a source replica (any remaining peer)
        if len(backup_table[filename]) == 0:
            print(f"[SERVER] WARNING: {filename} now has 0 replicas!")
            continue

        source_entry = backup_table[filename][0]    # any surviving replica

        # Select new target peer
        existing = {e["peer"] for e in backup_table[filename]}
        new_target = None
        for p in peers.keys():
            if p not in existing and p != dead_peer:
                new_target = p
                break

        if not new_target:
            print(f"[SERVER] No peer available for replication of {filename}")
            continue

        # Send replication request
        send_replicate_request(filename, source_entry, new_target, sock)


def choose_new_target_peer(exclude):
    for p in peers.keys():
        if p not in exclude:
            return p
    return None

def send_replicate_request(filename, source_entry, target_peer, sock):
    """
    Creates REPLICATE_REQ message:
    REPLICATE_REQ rq filename owner sourcePeer sourceIP sourceTCP
    """
    rq = 0  # no tracking needed for now
    owner = source_entry["owner"]
    source_peer = source_entry["peer"]
    source_ip = source_entry["ip"]
    source_tcp = source_entry["tcp"]

    msg = f"REPLICATE_REQ {rq} {filename} {owner} {source_peer} {source_ip} {source_tcp}"

    target_info = peers[target_peer]
    target_addr = (target_info["IP"], int(target_info["UDP_Port"]))

    print(f"[SERVER] Sending replication request to {target_peer}: {msg}")
    sock.sendto(msg.encode(), target_addr)

def handleRestoreRequest(msg, addr, sock):
    if len(msg) != 3:
        sock.sendto("RESTORE-DENIED 0 Invalid_Format".encode(), addr)
        return

    _, rq, filename = msg

    if filename not in backup_table or len(backup_table[filename]) == 0:
        reply = f"RESTORE-DENIED {rq} File_Not_Found"
        sock.sendto(reply.encode(), addr)
        return

    peer_entries = []
    for entry in backup_table[filename]:
        peer_name = entry["peer"]
        tcp_port = entry["tcp"]
        peer_entries.append(f"{peer_name}:{tcp_port}")

    peer_list_str = ",".join(peer_entries)

    reply = f"RESTORE_PLAN {rq} {filename} {peer_list_str}"
    sock.sendto(reply.encode(), addr)

def handleReplicateDone(msg, addr, sock):
    print("[SERVER] Received replication confirmation:", msg)

    if len(msg) != 5:
        return

    _, rq, filename, owner, peer = msg

    # Update backup table
    if filename not in backup_table:
        backup_table[filename] = []

    entry = {
        "peer": peer,
        "owner": owner,
        "ip": peers[peer]["IP"],
        "tcp": peers[peer]["TCP_Port"]
    }

    backup_table[filename].append(entry)

    print(f"[SERVER] Updated backup table with new replica for {filename}: {entry}")

def server_thread():
    #Main UDP server loop
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    print(f"[SERVER] Running on {SERVER_IP}:{SERVER_PORT}")

    # Start heartbeat monitoring
    threading.Thread(target=heartbeat_watchdog, args=(sock,), daemon=True).start()


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
