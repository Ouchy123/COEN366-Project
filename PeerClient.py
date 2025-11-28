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

    
    # UDP listener (RESTORE_PLAN handled here)
    def listen_UDP_responses(self):
        while self.running:
            try:
                print("\nListening for UDP responses...")
                data, adr = self.UDP_sock.recvfrom(1024)
                message = data.decode().strip()
                print(f"\n[CLIENT - UDP] Received: {message}")

                decoded = message.split()

                # RESTORE PLAN handling
                if decoded[0] == "RESTORE_PLAN":
                    _, rq, filename, peer_list_raw = decoded
                    peer_list = peer_list_raw.strip("[]").split(",")
                    self.requestTCPRestore(decoded)

                    print(f"[CLIENT] Restore plan received. Peers: {peer_list}")
                    self.download_chunks(filename, peer_list)

                if(decoded[0] == "PING"):
                    self.UDP_sock.sendto("PONG".encode(), adr)
                if(decoded[0]=="STORAGE_TASK"):
                    print("[CLIENT] Received STORAGE_TASK:", message)
                if(decoded[0]=="BACKUP-PLAN"):
                    print("[CLIENT] Received BACKUP-PLAN:", message)
                    self.requestTCPBackup(message)
                if(decoded[0]=="BACKUP-DENIED"):
                    print("[CLIENT] Received BACKUP-DENIED. Reason is: ", decoded[-1])
                    

            except socket.timeout:
                continue
            except Exception as e:
                print("[CLIENT] UDP Listener Error:", e)
                break
    
    #Client Handling of requests
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

    
    # TCP server for storage peers
    
    def start_storage_TCP_server(self):
        self.TCP_sock.listen()

        while True:
            conn, addr = self.TCP_sock.accept()
            print("[CLIENT - TCP] Connection from:", addr)
            threading.Thread(target=self.handle_TCP_reception,args=(conn,),daemon=True,).start()


    def requestTCPRestore(self,message):
        _, rq, filename, peer_list_raw = message
        peer_list = peer_list_raw[:-1].split("|")
        size = os.path.getsize(filename)
        for i,peer in enumerate(peer_list):
            peerInfo=peer.split(",")
            peerName,peerIP,peerTCPPort=peerInfo
            msg=f"GET_CHUNK RQ {filename} {i} {self.name} {self.ip},{self.tcp_port} {size}\n"
            print("[CLIENT] - Requesting chunk from peer: ",peerName," at IP:",peerIP," and TCP Port:",peerTCPPort)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((peerIP,int(peerTCPPort)))
                s.sendall(msg.encode())

    def requestTCPBackup(self,message):
        splitMsg=message.split()
        _,rq,filename,backupPeers,size=splitMsg
        backupPeers=backupPeers[:-1].split("|")
        print("[CLIENT] - Preparing the chunks from the file: ",filename, " with peers: ",backupPeers,"...")
        chunks=[]
        fileSize = os.path.getsize(filename)
        count=len(backupPeers)
        nb=0
        with open(filename,"rb") as f:
            while True:
                if count>1:
                    data=f.read(int(size))
                    nb+=nb
                    count-=count
                elif count==1:
                    data=f.read(fileSize - (nb*int(size)))
                else:
                    data=f.read()
                    if not data:
                        break
                chunks.append(data)

        print("[CLIENT] - The chunks have been successgully divided and ready to send. Total Chunks: ",len(chunks))
        print("[CLIENT] - Sending the chunks to the storage peers...")
        for i,peer in enumerate(backupPeers):
            peerInfo=peer.split(",")
            peerName,peerIP,peerTCPPort=peerInfo
            msg=f"SEND_CHUNK RQ {filename} {i} {self.name} {self.ip},{self.tcp_port} {chunks[i].__len__()}"
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((peerIP,int(peerTCPPort)))
                header=f"{msg} {zlib.crc32(chunks[i]) & 0xFFFFFFFF}\n"
                s.sendall(header.encode()+chunks[i])

    def recv_until_newline(self, conn):
        data = b""
        while not data.endswith(b"\n"):
            packet = conn.recv(1)
            if not packet:
                break
            data += packet
        return data.decode().strip()

    def recv_exact(self, conn, size):
        data = b""
        while len(data) < size:
            packet = conn.recv(size - len(data))
            if not packet:
                break
            data += packet
        return data

    def handle_TCP_reception(self, conn):
        try:

            headerLine = self.recv_until_newline(conn) #conn.recv(1024).decode().strip().split()
            header = headerLine.split()
            print("We are receiving a message, here is the hader: ",header)
            if(header[0]=="CHUNK_OK"):
                print("[CLIENT - TCP] Chunk stored successfully confirmation received:", headerLine)
                return
            if(header[0] =="SEND_CHUNK"):
                _, rq, filename, chunk_id, owner, addr, size, checSum= header
                addr=addr.split(",")
                addr=(addr[0],int(addr[1]))
                chunk_id = int(chunk_id)
                checksum = int(checSum)
                size = int(size)

                print("[CLIENT - TCP] Chunk request received for:", filename, " Chunk ID:", chunk_id)

                chunk_data = self.recv_exact(conn, size)

                # 3. Validate checksum
                calc_crc = zlib.crc32(chunk_data) & 0xFFFFFFFF

                if calc_crc != checksum:
                    print("[CLIENT - TCP] ERROR: Checksum mismatch, chunk corrupted!")
                    return
                print("[CLIENT - TCP] Checksum validated. Processing received chunk...")

                os.makedirs(f"chunks/{self.name}", exist_ok=True)
                chunk_path = f"chunks/{self.name}/{owner}-{filename}-{chunk_id}"

                # 5. Save chunk to disk
                with open(chunk_path, "wb") as f:
                    f.write(chunk_data)

                print(f"[CLIENT - TCP] Stored chunk at {chunk_path}")
                msg = f"CHUNK_OK RQ {filename} {self.name} {chunk_id}"
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(addr)
                    s.sendall(msg.encode())

            if(header[0]=="GET_CHUNK"):
                _, rq, filename, chunk_id, owner, addr, size= header
                addr=addr.split(",")
                addr=(addr[0],int(addr[1]))
                chunk_id=int(chunk_id)
                chunk_path=f"chunks/{self.name}/{owner}-{filename}-{chunk_id}"
                print("[CLIENT - TCP] Chunk request received for:", filename, " Chunk ID:", chunk_id)
                if not os.path.isfile(chunk_path):
                    print("[CLIENT - TCP] ERROR: Requested chunk file not found:", chunk_path)
                    return
                with open(chunk_path,"rb") as f:
                    chunk_data=f.read()
                checksum=zlib.crc32(chunk_data) & 0xFFFFFFFF
                msg=f"CHUNK_DATA RQ {filename} {chunk_id} {checksum} {size}\n"
                print(f"[CLIENT - TCP] Restore: Sent chunk {chunk_id} of file {filename}")
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((addr[0],int(addr[1])))
                    s.sendall(msg.encode()+chunk_data)
            
            if(header[0]=="CHUNK_DATA"):
                _, rq, filename, chunk_id, chekSum, size= header
                print("[CLIENT - TCP] Receiving chunk data...")
                chunk_data = self.recv_exact(conn, int(size))
                with open(filename, "ab") as f:
                    f.write(chunk_data)

                print(f"[CLIENT - TCP] Restored - Stored chunk at {filename}")
                

            
                

        except Exception as e:
            print("[TCP] ERROR receiving chunk:", e)

        finally:
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
