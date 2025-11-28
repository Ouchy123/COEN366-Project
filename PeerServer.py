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

def handleRegistration(msg,addr,sock):
    
    print("\n[SERVER] : At handleRegistration\n")
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
            peers[name]["Role"] = role
            peers[name]["IP"] = ip
            peers[name]["UDP_Port"] = udp_port
            peers[name]["TCP_Port"] = tcp_port
            peers[name]["Storage"] = storage
            save_db()
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
    if len(msg) != 5:
        reply = f"BACKUP-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
        sock.sendto(reply.encode(), addr)
        return
    
    msgWithoutCRC = ' '.join(msg[:-1])
    calculated_crc = zlib.crc32(msgWithoutCRC.encode()) & 0xFFFFFFFF
    if str(calculated_crc) == msg[-1]:
        print("[SERVER] CRC32 Check Passed")

    _, name, filename, size, received_crc = msg
    print(f"[SERVER] Backup request from {name} for file {filename} of size {size} bytes")
    with lock:
        if filename not in backup_table:
            backup_table[filename] = []
        else:
            print("[SERVER] File already exists in backup table, therefore we cannot do a double backup.")
            msg = f"BACKUP-DENIED RQ {filename} File_Already_Backed_Up."
            sock.sendto(msg.encode(), addr)
            return
        if name not in backup_table[filename]:
            backup_table[filename].append(name)
            
    print("[SERVER] Updated backup table:", backup_table)
    
    #Check for peers with storage role and sufficient space
    potentialPeer=[]
    backupPeer={}
    strPeers=""
    global peers
    with lock:
        for peer, peer_info in peers.items():
            print("[Server] - Checking peer: ",peer, ". Role is: ",peer_info["Role"]," and storage is: ", peer_info["Storage"])
            if(peer_info["Role"] in ["storage", "both"] and peer != name):
                potentialPeer.append(peer)
        
        count = len(potentialPeer)
        while count !=0:
            for peer in potentialPeer:
                if(int(peers[peer]["Storage"])>= int(size)//count):
                    print("[SERVER] - Peer ",peer," has been added temporarily")
                    pass
                    
                else:
                    potentialPeer.remove(peer)
                    count=len(potentialPeer)
                    break
            count=0

        for peer in potentialPeer:
            print("[Server] - Peer ",peer," is eligible for backup")
            backupPeer[peer]=f"{peer},{peers[peer]["IP"]},{peers[peer]["TCP_Port"]}|"
            strPeers+=str(backupPeer[peer])
            
        print("[Server] - Printing the backupPeer: ",backupPeer)
        chunk_size =int(size)//len(backupPeer)
        decimal_chunk_size=int(size)/len(backupPeer)
        for peer in backupPeer:
            sock.sendto(f"STORAGE_TASK RQ {filename} Owner:{name} {chunk_size}".encode(), (peers[peer]["IP"], int(peers[peer]["UDP_Port"])))
    replyRequester = f"BACKUP-PLAN RQ {filename} {strPeers} {chunk_size}"
    sock.sendto(replyRequester.encode(), addr)
    print(f"[SERVER] Sent backup plan to requester {name}: {replyRequester}")


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
    if len(msg) != 3:
        sock.sendto("RESTORE-DENIED 0 Invalid_Format".encode(), addr)
        return

    _, rq, filename = msg

    # TODO: Replace this with your real table that tracks chunks
    if filename not in backup_table:
        reply = f"RESTORE-DENIED {rq} File_Not_Found"
    else:
        peer_list = backup_table[filename]
        reply = f"RESTORE_PLAN {rq} {filename} {peer_list}"

    sock.sendto(reply.encode(), addr)

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
