import socket
import threading
import time
import sys

SERVER_IP = "127.0.0.1"
SERVER_PORT = 5000

class Peer:
    def __init__(self, name, role, udp_port, tcp_port, storage):
        self.name = name
        self.role = role
        self.ip = "127.0.0.1"
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.storage = storage
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.udp_port))
        self.running = True

    def listen_responses(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(1024)
                print(f"[CLIENT] Received: {data.decode().strip()}")
            except Exception:
                break

    def register(self):
        msg = f"REGISTER 01 {self.name} {self.role} {self.ip} {self.udp_port} {self.tcp_port} {self.storage}"
        self.sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")

    def deregister(self):
        msg = f"DE-REGISTER 02 {self.name}"
        self.sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")

    def run(self):
        listener = threading.Thread(target=self.listen_responses, daemon=True)
        listener.start()

        self.register()
        time.sleep(2) 
        input("Press ENTER to deregister and exit\n")
        self.deregister()
        time.sleep(1)
        self.running = False
        self.sock.close()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python peer.py <Name> <Role> <UDP_Port> <TCP_Port> <Storage>")
        sys.exit(1)

    name, role, udp, tcp, storage = sys.argv[1:]
    peer = Peer(name, role, int(udp), int(tcp), storage)
    peer.run()
