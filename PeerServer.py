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
backup_table = {}  # {filename: [entries]}


def save_db():
    try:
        with open(DB_FILE, "w") as f:
            json.dump(peers, f, indent=4)
    except Exception as e:
        print(f"[SERVER] Error saving database: {e}")


def handleRegistration(msg, addr, sock):
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

    msgWithoutCRC = " ".join(msg[:-1])
    calculated_crc = zlib.crc32(msgWithoutCRC.encode()) & 0xFFFFFFFF
    if str(calculated_crc) == msg[-1]:
        print("[SERVER] CRC32 Check Passed")

    _, owner, filename, size_str, received_crc = msg

    # --- NEW: choose storage peers and send STORAGE_TASK + BACKUP_PLAN ---
    try:
        file_size = int(size_str)
    except ValueError:
        reply = "BACKUP-DENIED 0 Invalid_Size"
        sock.sendto(reply.encode(), addr)
        return

    with lock:
        candidate_peers = []
        for pname, info in peers.items():
            role = info["Role"].lower()
            if role in ["storage", "both"]:
                candidate_peers.append(pname)

        if not candidate_peers:
            reply = "BACKUP-DENIED 0 No_Storage_Peers"
            sock.sendto(reply.encode(), addr)
            return

        chunk_size = file_size  # simplest: one chunk = whole file

        if filename not in backup_table:
            backup_table[filename] = []

        backup_table[filename] = []
        for pname in candidate_peers:
            info = peers[pname]
            entry = {
                "peer": pname,
                "owner": owner,
                "ip": info["IP"],
                "udp": info["UDP_Port"],
                "tcp": info["TCP_Port"],
            }
            backup_table[filename].append(entry)

    # send STORAGE_TASK to each selected storage peer
    for entry in backup_table[filename]:
        p_ip = entry["ip"]
        p_udp = int(entry["udp"])
        storage_msg = f"STORAGE_TASK 0 {filename} {chunk_size} {owner}"
        sock.sendto(storage_msg.encode(), (p_ip, p_udp))

    peer_names = [e["peer"] for e in backup_table[filename]]
    peer_list_str = "[" + ",".join(peer_names) + "]"
    reply = f"BACKUP_PLAN 0 {filename} {peer_list_str} {chunk_size}"
    sock.sendto(reply.encode(), addr)
    print("[SERVER] Sent BACKUP_PLAN:", reply)


def handle_message(data, addr, sock):
    msg = data.decode().strip().split()
    if not msg:
        return

    cmd = msg[0].upper()
    print("At handle_message and cmd received is:", cmd)

    match cmd:
        case "REGISTER":
            print("At REGISTER case")
            handleRegistration(msg, addr, sock)

        case "DE-REGISTER":
            print("At DE-REGISTER case")
            handleDeregistration(msg, addr, sock)

        case "BACKUP-REQUEST":
            handleBackupRequest(msg, addr, sock)

        case "HEARTBEAT":
            handleHeartbeat(msg, addr, sock)

        case "RESTORE_REQ":
            handleRestoreRequest(msg, addr, sock)

        case "REPLICATE_DONE":
            handleReplicateDone(msg, addr, sock)

        case _:
            print(f"[SERVER] Unknown command from {addr}: {data.decode().strip()}")
            sock.sendto("ERROR Unknown_Command".encode(), addr)


def handleHeartbeat(msg, addr, sock):
    print("[SERVER] Received HEARTBEAT:", msg)

    if len(msg) != 5:
        print("[SERVER] HEARTBEAT: Invalid format")
        return

    _, rq, name, chunk_count, timestamp = msg

    with lock:
        heartbeat_table[name] = int(timestamp)


def heartbeat_watchdog(sock):
    # watches peers and triggers failure handling (2.5)
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
    # simple replication trigger (control only)
    print(f"[SERVER] Handling failure of {dead_peer}...")

    for filename, entry_list in backup_table.items():
        lost = [e for e in entry_list if e["peer"] == dead_peer]
        if not lost:
            continue

        print(f"[SERVER] Peer {dead_peer} had a replica of {filename}")
        backup_table[filename] = [e for e in entry_list if e["peer"] != dead_peer]

        if len(backup_table[filename]) == 0:
            print(f"[SERVER] WARNING: {filename} now has 0 replicas!")
            continue

        source_entry = backup_table[filename][0]

        existing = {e["peer"] for e in backup_table[filename]}
        new_target = None
        for p in peers.keys():
            if p not in existing and p != dead_peer:
                new_target = p
                break

        if not new_target:
            print(f"[SERVER] No peer available for replication of {filename}")
            continue

        send_replicate_request(filename, source_entry, new_target, sock)


def send_replicate_request(filename, source_entry, target_peer, sock):
    rq = 0
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
    else:
        peer_names = [e["peer"] for e in backup_table[filename]]
        peer_list_str = "[" + ",".join(peer_names) + "]"
        reply = f"RESTORE_PLAN {rq} {filename} {peer_list_str}"

    sock.sendto(reply.encode(), addr)


def handleReplicateDone(msg, addr, sock):
    # server-side acknowledgement of REPLICATE_DONE
    print("[SERVER] Received replication confirmation:", msg)
    if len(msg) != 5:
        return

    _, rq, filename, owner, peer = msg

    with lock:
        if filename not in backup_table:
            backup_table[filename] = []

        if peer not in peers:
            print("[SERVER] REPLICATE_DONE from unknown peer", peer)
            return

        info = peers[peer]
        entry = {
            "peer": peer,
            "owner": owner,
            "ip": info["IP"],
            "udp": info["UDP_Port"],
            "tcp": info["TCP_Port"],
        }
        backup_table[filename].append(entry)
        print(f"[SERVER] Updated backup table with new replica for {filename}: {entry}")


def server_thread():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    print(f"[SERVER] Running on {SERVER_IP}:{SERVER_PORT}")

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
