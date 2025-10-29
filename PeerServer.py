import socket
import threading
import json
import os

SERVER_IP = "127.0.0.1"
SERVER_PORT = 5000 
DB_FILE = "registered_peers.json" #database for the registered peers

lock = threading.Lock() #only one thread can write to the file

#Load persistent registration
if os.path.exists(DB_FILE): #checks if the file (database) exists
    with open(DB_FILE, "r") as f:
        peers = json.load(f)
else:
    peers = {} #starts with empty dictionary

#function to save registered peers
def save_db():
    with lock:
        with open(DB_FILE, "w") as f:
            json.dump(peers, f, indent=4)

#function that handles one incoming UDP message from client
def handle_message(data, addr, sock):
    msg = data.decode().strip().split()
    if not msg:
        return
    cmd = msg[0].upper()

    #handles registration
    if cmd == "REGISTER":
        #getting format: REGISTER RQ# Name Role IP UDP_PORT TCP_PORT Storage
        if len(msg) != 8:
            reply = f"REGISTER-DENIED {msg[1] if len(msg)>1 else 0} Invalid_Format"
            sock.sendto(reply.encode(), addr)
            return
        
        _, rq, name, role, ip, udp_port, tcp_port, storage = msg

        with lock:
            if name in peers:
                reply = f"REGISTER-DENIED {rq} NameAlreadyInUse"
            else:
                peers[name] = {
                    "Role": role,
                    "IP": ip,
                    "UDP_Port": udp_port,
                    "TCP_Port": tcp_port,
                    "Storage": storage
                }    
                save_db()
                reply = f"REGISTERED {rq}"

        print(f"[SERVER] {name} -> {reply}")
        sock.sendto(reply.encode(), addr)

    #handles de-registration
    elif cmd == "DE-REGISTER":
        #getting format: DE-REGISTER RQ# Name
        if len(msg) != 3:
            return
        _, rq, name = msg
        with lock:
            if name in peers:
                del peers[name]
                save_db()
                print(f"[SERVER] {name} deregistered.")
            else:
                print(f"[SERVER] Unknown peer {name} tried to deregistered.")
    else:
        print(f"[SERVER] Unknown command from {addr}: {data.decode().strip()}")

def server_thread():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    print(f"[SERVER] Running on {SERVER_IP}:{SERVER_PORT}")

    while True:
        data, addr = sock.recvfrom(1024)
        threading.Thread(target=handle_message, args=(data, addr, sock)).start()

if __name__ == "__main__":
    server_thread()

