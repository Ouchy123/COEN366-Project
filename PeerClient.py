import socket
import threading
import os
import sys
import zlib
import time

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

        # UDP socket
        self.UDP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.UDP_sock.bind((self.ip, self.udp_port))
        self.UDP_sock.settimeout(60.0)

        # TCP socket
        self.TCP_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.TCP_sock.bind((self.ip, self.tcp_port))

        self.running = True
        self.registered = False
        self.reqNum = (name, 0)

    
   def listen_UDP_responses(self):
    while self.running:
        try:
            print("\nListening for UDP responses...")
            data, _ = self.UDP_sock.recvfrom(1024)
            message = data.decode().strip()
            print(f"\n[CLIENT] Received: {message}")

            decoded = message.split()
            if not decoded:
                continue

            cmd = decoded[0]

            if cmd == "RESTORE_PLAN":
                _, rq, filename, peer_list_raw = decoded
                peer_list = peer_list_raw.strip("[]").split(",")

                print(f"[CLIENT] Restore plan received. Peers: {peer_list}")

                self.download_chunks(filename, peer_list)

            elif cmd == "REPLICATE_REQ":
                if len(decoded) != 7:
                    print("[CLIENT] Invalid REPLICATE_REQ format.")
                    continue

                _, rq, filename, owner, source_peer, source_ip, source_tcp = decoded

                print(f"[CLIENT] REPLICATE_REQ received for {filename} from {source_peer}")

                self.handle_replication(
                    filename,
                    owner,
                    source_peer,
                    source_ip,
                    int(source_tcp)
                )

        except socket.timeout:
            continue
        except Exception as e:
            print("[CLIENT] UDP Listener Error:", e)
            break

    
    def register(self):
        if self.registered:
            print("[CLIENT] Already registered.")
            return
        msg = f"REGISTER 01 {self.name} {self.role} {self.ip} {self.udp_port} {self.tcp_port} {self.storage}"
        self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")
        self.registered = True

    
    def deregister(self):
        if not self.registered:
            print("[CLIENT] You are not registered.")
            return
        msg = f"DE-REGISTER 02 {self.name}"
        self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")
        self.registered = False

   
    def get_status(self):
        status = {
            "Name": self.name,
            "Role": self.role,
            "UDP_Port": self.udp_port,
            "TCP_Port": self.tcp_port,
            "Storage": self.storage,
            "Registered": self.registered,
        }
        print(f"[CLIENT] Status: {status}")
        return status

    
    def exit(self):
        if self.registered:
            self.deregister()
        print("[CLIENT] Exiting...")
        self.running = False
        self.UDP_sock.close()
        self.TCP_sock.close()

   
    # BACKUP
    
    def backup_request(self):
        while True:
            path = input("Enter the file path to back up: ").strip()

            if os.path.isfile(path):
                print("File found:", path, "\nStarting backup process...")
                size = os.path.getsize(path)

                msg = f"BACKUP-REQUEST {self.name} {os.path.basename(path)} {size}"
                msg = msg + f" {self.crc32_backup(msg.encode())}"

                print("[CLIENT] Sending backup request:", msg)
                self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
                break

            elif path.lower() == "exit":
                print("Exiting backup request.")
                break

            else:
                print("File not found")

   
    # RESTORE REQUEST
    
    def restore_request(self):
        filename = input("Enter filename to restore: ").strip()
        rq = self.reqNum[1]
        msg = f"RESTORE_REQ {rq} {filename}"
        self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {msg}")

    
    def crc32_backup(self, msg: bytes):
        return zlib.crc32(msg) & 0xFFFFFFFF

   
    # HEARTBEAT
    
    def send_heartbeat(self):
        while self.running:
            if self.registered:
                ts = int(time.time())
                msg = f"HEARTBEAT {self.reqNum[1]} {self.name} 0 {ts}"
                try:
                    self.UDP_sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
                    print(f"[CLIENT] Sent heartbeat: {msg}")
                except:
                    pass
            time.sleep(5)

    
    # TCP chunk downloader (temporary stub)
    
    def download_chunks(self, filename, peer_list):
        print(f"[CLIENT] Starting file restore for: {filename}")
        print(f"[CLIENT] (TEMP) Not actually downloading chunks yet.")
        print(f"[CLIENT] Would connect to peers: {peer_list}")
        print("[CLIENT] Restore complete (stub).")

    def handle_replication(self, filename, owner, source_peer, source_ip, source_tcp):
    print(f"[CLIENT] Starting replication for {filename} "
          f"from {source_peer}@{source_ip}:{source_tcp}")

    try:
        # Connect to the source via TCP
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((source_ip, source_tcp))

        # Request chunk (in your system: entire file)
        rq = self.reqNum[1]  # you are not incrementing reqNum, but matching your code
        msg = f"GET_CHUNK {rq} {filename}"
        s.send(msg.encode())

        # Receive header
        header = s.recv(1024).decode().strip().split()
        if len(header) < 5 or header[0] != "CHUNK_DATA":
            print("[CLIENT] Invalid chunk header during replication:", header)
            return

        _, _, _, chunk_id, checksum = header

        # Receive chunk
        chunk = s.recv(999999)

        # Verify checksum
        actual = zlib.crc32(chunk) & 0xFFFFFFFF
        if actual != int(checksum):
            print("[CLIENT] Replication checksum mismatch!")
            return

        # Save replica locally
        save_path = f"chunks/{filename}_replica"
        with open(save_path, "wb") as f:
            f.write(chunk)

        print(f"[CLIENT] Replication complete. Saved at {save_path}")

        # Notify server
        ack = f"REPLICATE_DONE {rq} {filename} {owner} {self.name}"
        self.UDP_sock.sendto(ack.encode(), (SERVER_IP, SERVER_PORT))
        print(f"[CLIENT] Sent: {ack}")

    except Exception as e:
        print("[CLIENT] Replication Error:", e)

    finally:
        try:
            s.close()
        except:
            pass
    
    # TCP server for storage peers
    
    def start_storage_TCP_server(self):
        self.TCP_sock.listen()

        while True:
            conn, addr = self.TCP_sock.accept()
            threading.Thread(
                target=self.handle_TCP_chunk_request,
                args=(conn,),
                daemon=True,
            ).start()


    def handle_TCP_chunk_request(self, conn):
        header = conn.recv(1024).decode().strip().split()
        _, rq, filename, chunk_id = header

        chunk_path = f"chunks/{filename}_chunk{chunk_id}"

        if not os.path.exists(chunk_path):
            print(f"[CLIENT] Missing chunk: {chunk_path}")
            conn.close()
            return

        with open(chunk_path, "rb") as f:
            data = f.read()

        checksum = zlib.crc32(data) & 0xFFFFFFFF
        response_header = f"CHUNK_DATA {rq} {filename} {chunk_id} {checksum}\n"

        conn.sendall(response_header.encode() + data)
        conn.close()

   
    def run(self):
        # Start threads
        threading.Thread(target=self.listen_UDP_responses, daemon=True).start()
        threading.Thread(target=self.start_storage_TCP_server, daemon=True).start()
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

        print(f"\n[CLIENT] Peer '{self.name}' is active.")
        print("[CLIENT] Available commands:")
        print("  register")
        print("  deregister")
        print("  status")
        print("  backup")
        print("  restore")
        print("  exit\n")

        while self.running:
            cmd = input("Command: ").strip().lower()

            match cmd:
                case "register":
                    self.register()
                case "deregister":
                    self.deregister()
                case "status":
                    self.get_status()
                case "backup":
                    self.backup_request()
                case "restore":
                    self.restore_request()
                case "exit":
                    self.exit()
                case _:
                    print("[CLIENT] Unknown command.")



def get_free_TCP_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_free_UDP_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]



if __name__ == "__main__":
    UDP_PORT = get_free_UDP_port()
    TCP_PORT = get_free_TCP_port()

    credentials = input("Enter credentials (Name Role): ").split()

    try:
        while True:
            if credentials[1].lower() in ["owner", "storage", "both"]:
                break
            credentials[1] = input("Invalid role. Enter owner/storage/both: ")

        name, role = credentials[:2]
        peer = Peer(name, role, UDP_PORT, TCP_PORT, 1024)
        peer.run()

    except KeyboardInterrupt:
        peer.exit()
