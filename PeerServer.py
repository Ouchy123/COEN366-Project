import socket
import threading
import json
import os

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


#saving to json database
def save_db():
    #Save the current peer dictionary to disk
    try:
        with open(DB_FILE, "w") as f:
            json.dump(peers, f, indent=4)
    except Exception as e:
        print(f"[SERVER] Error saving database: {e}")


#function for handling messages
def handle_message(data, addr, sock):
    msg = data.decode().strip().split()
    if not msg:
        return

    cmd = msg[0].upper()

    # Handle registration
    if cmd == "REGISTER":
        if len(msg) != 8:
            reply = f"REGISTER-DENIED {msg[1] if len(msg) > 1 else 0} Invalid_Format"
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
                    "Storage": storage,
                }
                save_db()
                reply = f"REGISTERED {rq}"

        print(f"[SERVER] {name} -> {reply}")
        sock.sendto(reply.encode(), addr)

    # Handle de-registration
    elif cmd == "DE-REGISTER":
        if len(msg) != 3:
            return
        _, rq, name = msg
        with lock:
            if name in peers:
                del peers[name]
                save_db()
                print(f"[SERVER] {name} deregistered.")
                sock.sendto(f"DE-REGISTERED {rq}".encode(), addr)
            else:
                print(f"[SERVER] Unknown peer {name} tried to deregister.")

    else:
        print(f"[SERVER] Unknown command from {addr}: {data.decode().strip()}")


def server_thread():
    #Main UDP server loop
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, SERVER_PORT))
    print(f"[SERVER] Running on {SERVER_IP}:{SERVER_PORT}")

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
