import socket
import threading
import os
import sys
import zlib

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
        # Create UDP and TCP sockets
        self.UDP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.UDP_sock.bind((self.ip, self.udp_port))
        self.UDP_sock.settimeout(60.0)

        self.TCP_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.TCP_sock.bind((self.ip, self.tcp_port))
        self.TCP_sock.settimeout(60.0)

        self.running = True
        self.registered = False
        self.reqNum = (name,0)

    def listen_UDP_responses(self):
        #Continuously listen for server responses
        while self.running:
            try:
                print( "\nListening for UDP responses..." )
                data, _ = self.UDP_sock.recvfrom(1024)
                print(f"\n[CLIENT] Received: {data.decode().strip()}")
            except socket.timeout:
                continue
            except Exception:
                break

    def listen_TCP_responses(self):
        #Continuously listen for server responses
        while self.running:
            try:
                print( "\nListening for TCP responses..." )
                self.data, _ = self.TCP_sock.recvfrom(1024)
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
        self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")
        self.registered = True

    def deregister(self):
        #Remove this peer from the server
        if not self.registered:
            print("[CLIENT] You are not registered.")
            return
        msg = f"DE-REGISTER 02 {self.name}"
        self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")
        self.registered = False

    def get_status(self):
        status = {"Name": self.name, "Role": self.role,
                  "UDP_Port": self.udp_port, "TCP_Port": self.tcp_port,
                  "Storage": self.storage, "Registered": self.registered}
        print(f"[CLIENT] Status: {status}")
        return status
    
    def exit(self):
        #Close connections and exit
        if self.registered:
            self.deregister()
        print("[CLIENT] Exiting...")
        self.running = False
        self.UDP_sock.close()
        self.TCP_sock.close()


    #Backup Requests:
    def backup_request(self):
        while(True):
            path = input("Enter the file path to back up: ").strip()
            if os.path.isfile(path):
                print("File found:", path , "\nStarting backup process...")
                size = os.path.getsize(path)
                msg = f"BACKUP-REQUEST {self.name} {os.path.basename(path)} {size}"
                msg = msg + f" {self.crc32_backup(msg.encode())}"
                print("we are in the backup request and here is the message: ", msg)

                self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
                break
            elif path.lower() == "exit":
                print("Exiting backup request.")
                break
            else:
                print("File not found")

    def crc32_backup(self, msg:bytes):
        return zlib.crc32(msg) & 0xFFFFFFFF

    def run(self):
        #Main command loop for the peer
        UDP_listener = threading.Thread(target=self.listen_UDP_responses, daemon=True)
        UDP_listener.start()

        TCP_listener = threading.Thread(target=self.listen_TCP_responses, daemon=True)
        TCP_listener.start()

        print(f"\n[CLIENT] Peer '{self.name}' is active.")
        print("[CLIENT] Available commands:")
        print("  register   → register this peer with the server")
        print("  deregister → remove this peer from the server")
        print("  status     → show peer info")
        print("  exit       → close connection and quit\n")

        while self.running:
            cmd = input("\nEnter the command you would like to execute:\n1)Register\n2)Deregister\n3)Status\n4)Backup\n5)Exit\nCommand:").strip().lower()
            match cmd.lower():
                case "register":
                    self.register()

                case "deregister":
                    self.deregister()

                case "status":
                    self.get_status()

                case "exit":
                    self.exit()
                
                case "backup":
                    self.backup_request()

                case _:
                    print("[CLIENT] Unknown command. Try again among the commands mentionned:")

def get_free_TCP_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        print("--- Free TCP Port:", s.getsockname()[1])
        return s.getsockname()[1]

def get_free_UDP_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("", 0))
        print("--- Free UDP Port:", s.getsockname()[1])
        return s.getsockname()[1]
    
if __name__ == "__main__":
    # Find free ports
    UDP_PORT = get_free_UDP_port()
    TCP_PORT = get_free_TCP_port()
    

    credentials = (input("Enter credentials (format: Name Role): ")).split()
    try:
        while True:
            if credentials[1].lower() in ["owner", "storage","both"]:
                break
            else:
                credentials[1] = input("Invalid role. Please enter 'owner', 'storage' or 'Both':")

        print(credentials)

        name, role, udp, tcp, storage = *credentials[:2], UDP_PORT, TCP_PORT, 1024
        peer = Peer(name, role, int(udp), int(tcp), storage)
        peer.run()
    except KeyboardInterrupt:
        peer.exit()
    
