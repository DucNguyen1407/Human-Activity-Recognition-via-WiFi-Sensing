"""
server2server.py

Kiến trúc:

    Client A1 \
    Client A2 ----> Server A ----forward----> Client B
    Client A3 /

- Server A:
    + TCP server
    + tối đa 3 client
    + nhận JSON kết thúc bởi '\n'

- Server B:
    + TCP server
    + chỉ 1 client
    + nhận dữ liệu forward từ A

Python >= 3.8
"""

import socket
import threading

HOST = "0.0.0.0"

PORT_A = 9001
PORT_B = 9100

MAX_CLIENT_A = 3
BUFFER_SIZE = 4096


# ============================================================
# Global state
# ============================================================

clients_a = []
clients_a_lock = threading.Lock()

client_b = None
client_b_lock = threading.Lock()


# ============================================================
# Forward sang client ở server B
# ============================================================

def forward_to_b(data: bytes):
    global client_b

    with client_b_lock:
        if client_b is None:
            print("[B] No client connected")
            return

        try:
            client_b.sendall(data)

        except Exception as e:
            print(f"[B] Send error: {e}")

            try:
                client_b.close()
            except:
                pass

            client_b = None


# ============================================================
# Handle client của Server A
# ============================================================

def handle_client_a(conn: socket.socket, addr):

    print(f"[A] Client connected: {addr}")

    buffer = ""

    try:
        while True:

            data = conn.recv(BUFFER_SIZE)

            if not data:
                break

            buffer += data.decode("utf-8", errors="ignore")

            while "\n" in buffer:

                line, buffer = buffer.split("\n", 1)

                line = line.strip()

                if not line:
                    continue

                json_msg = line + "\n"

                print(f"[A] RX: {json_msg.strip()}")

                # forward sang client ở B
                forward_to_b(json_msg.encode())

    except Exception as e:
        print(f"[A] Error: {e}")

    finally:

        print(f"[A] Client disconnected: {addr}")

        with clients_a_lock:
            if conn in clients_a:
                clients_a.remove(conn)

        try:
            conn.close()
        except:
            pass


# ============================================================
# Server A
# ============================================================

def server_a_thread():

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOST, PORT_A))
    server.listen()

    print(f"[A] Listening on {PORT_A}")

    while True:

        conn, addr = server.accept()

        with clients_a_lock:

            if len(clients_a) >= MAX_CLIENT_A:

                print("[A] Reject client: max reached")

                conn.close()
                continue

            clients_a.append(conn)

        t = threading.Thread(
            target=handle_client_a,
            args=(conn, addr),
            daemon=True
        )

        t.start()


# ============================================================
# Server B
# ============================================================

def server_b_thread():

    global client_b

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOST, PORT_B))
    server.listen(1)

    print(f"[B] Listening on {PORT_B}")

    while True:

        conn, addr = server.accept()

        with client_b_lock:

            # chỉ cho phép 1 client
            if client_b is not None:

                print("[B] Reject extra client")

                conn.close()
                continue

            client_b = conn

        print(f"[B] Client connected: {addr}")

        try:

            while True:

                data = conn.recv(BUFFER_SIZE)

                if not data:
                    break

                print(f"[B] RX from B-client: {data.decode(errors='ignore').strip()}")

        except Exception as e:
            print(f"[B] Error: {e}")

        finally:

            print("[B] Client disconnected")

            with client_b_lock:
                try:
                    conn.close()
                except:
                    pass

                if client_b == conn:
                    client_b = None


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    threading.Thread(
        target=server_a_thread,
        daemon=True
    ).start()

    threading.Thread(
        target=server_b_thread,
        daemon=True
    ).start()

    print("Server2Server running...")

    while True:
        try:
            threading.Event().wait(1)

        except KeyboardInterrupt:
            print("Exit")
            break