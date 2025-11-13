import socket
import threading
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
        self.sock.settimeout(1.0)
        self.running = True
        self.registered = False

    def listen_responses(self):
        #Continuously listen for server responses
        while self.running:
            try:
                data, _ = self.sock.recvfrom(1024)
                print(f"\n[CLIENT] Received: {data.decode().strip()}")
            except socket.timeout:
                continue
            except Exception:
                break

    def register(self):
        #Register this peer with the server
        if self.registered:
            print("[CLIENT] Already registered.")
            return
        msg = f"REGISTER 01 {self.name} {self.role} {self.ip} {self.udp_port} {self.tcp_port} {self.storage}"
        self.sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")
        self.registered = True

    def deregister(self):
        #Remove this peer from the server
        if not self.registered:
            print("[CLIENT] You are not registered.")
            return
        msg = f"DE-REGISTER 02 {self.name}"
        self.sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")
        self.registered = False

    def run(self):
        #Main command loop for the peer
        listener = threading.Thread(target=self.listen_responses, daemon=True)
        listener.start()

        print(f"\n[CLIENT] Peer '{self.name}' is active.")
        print("[CLIENT] Available commands:")
        print("  register   → register this peer with the server")
        print("  deregister → remove this peer from the server")
        print("  status     → show peer info")
        print("  exit       → close connection and quit\n")

        while True:
            cmd = input("Command: ").strip().lower()

            if cmd == "register":
                self.register()
            elif cmd == "deregister":
                self.deregister()
            elif cmd == "status":
                print(
                    f"[CLIENT] Name: {self.name}, Role: {self.role}, "
                    f"UDP: {self.udp_port}, TCP: {self.tcp_port}, "
                    f"Storage: {self.storage}, Registered: {self.registered}"
                )
            elif cmd == "exit":
                if self.registered:
                    self.deregister()
                print("[CLIENT] Exiting...")
                self.running = False
                break
            else:
                print("[CLIENT] Unknown command. Try 'register', 'deregister', 'status', or 'exit'.")

        self.sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python PeerClient.py <Name> <Role> <UDP_Port> <TCP_Port> <Storage>")
        sys.exit(1)

    name, role, udp, tcp, storage = sys.argv[1:]
    peer = Peer(name, role, int(udp), int(tcp), storage)
    peer.run()
