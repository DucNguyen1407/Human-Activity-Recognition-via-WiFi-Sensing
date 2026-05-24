# collection_stub/tcp_stream_server.py
#
# TCP server mẫu cho nhóm Collection.
#
# Collection mở TCP server.
# CSI-IoT-system sẽ connect vào server này để đọc dữ liệu và có thể gửi lệnh
# điều khiển ngược lại cho Collection.
#
# Giao tiếp theo JSON Lines:
#   Server -> Client: {"device_id":"esp1",...}\n
#   Client -> Server: {"type":"uart_control",...}\n

import json
import socket
import threading
from typing import Callable, Optional


JsonHandler = Callable[[dict], None]
ClientHandler = Callable[[], None]


class TcpStreamServer:
    def __init__(self, host: str, port: int, name: str):
        self.host = host
        self.port = port
        self.name = name

        self.server_sock: Optional[socket.socket] = None
        self.client_sock: Optional[socket.socket] = None

        self.lock = threading.Lock()
        self.running = False

        self.message_handler: Optional[JsonHandler] = None
        self.client_connected_handler: Optional[ClientHandler] = None

    def set_message_handler(self, handler: JsonHandler):
        """
        Đăng ký hàm xử lý message JSON mà Management gửi xuống Collection.
        """
        self.message_handler = handler

    def set_client_connected_handler(self, handler: ClientHandler):
        """
        Đăng ký hàm chạy ngay sau khi Management connect vào TCP server.
        Ví dụ: gửi danh sách COM fake cho Management.
        """
        self.client_connected_handler = handler

    def start(self):
        """
        Mở TCP server và chờ client connect.
        """
        self.running = True

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(1)

        print(f"[{self.name}] TCP server listening on {self.host}:{self.port}")

        thread = threading.Thread(
            target=self._accept_loop,
            name=f"{self.name}-accept-loop",
            daemon=True,
        )
        thread.start()

    def _accept_loop(self):
        """
        Chờ CSI-IoT-system connect.
        """
        while self.running:
            try:
                client, addr = self.server_sock.accept()

                with self.lock:
                    if self.client_sock is not None:
                        try:
                            self.client_sock.close()
                        except Exception:
                            pass

                    self.client_sock = client

                print(f"[{self.name}] Client connected: {addr}")

                recv_thread = threading.Thread(
                    target=self._client_receive_loop,
                    args=(client,),
                    name=f"{self.name}-receive-loop",
                    daemon=True,
                )
                recv_thread.start()

                if self.client_connected_handler is not None:
                    try:
                        self.client_connected_handler()
                    except Exception as e:
                        print(f"[{self.name}] client_connected_handler error: {e}")

            except OSError:
                break
            except Exception as e:
                print(f"[{self.name}] accept error: {e}")

    def _client_receive_loop(self, client: socket.socket):
        """
        Nhận lệnh từ Management gửi xuống Collection theo JSON Lines.
        """
        buffer = b""

        while self.running:
            with self.lock:
                if client is not self.client_sock:
                    break

            try:
                chunk = client.recv(4096)

                if not chunk:
                    break

                buffer += chunk

                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)

                    if not line.strip():
                        continue

                    try:
                        message = json.loads(line.decode("utf-8"))
                    except json.JSONDecodeError as e:
                        print(f"[{self.name}] invalid JSON from client: {e}")
                        continue

                    if self.message_handler is not None:
                        try:
                            self.message_handler(message)
                        except Exception as e:
                            print(f"[{self.name}] message_handler error: {e}")

            except OSError:
                break
            except Exception as e:
                print(f"[{self.name}] receive error: {e}")
                break

        with self.lock:
            if client is self.client_sock:
                try:
                    client.close()
                except Exception:
                    pass
                self.client_sock = None

        print(f"[{self.name}] Client disconnected")

    def send_packet(self, packet: dict):
        """
        Gửi một packet/message dạng JSON line.
        """
        line = json.dumps(packet, ensure_ascii=False) + "\n"
        data = line.encode("utf-8")

        with self.lock:
            if self.client_sock is None:
                return False

            try:
                self.client_sock.sendall(data)
                return True

            except Exception as e:
                print(f"[{self.name}] send error: {e}")

                try:
                    self.client_sock.close()
                except Exception:
                    pass

                self.client_sock = None
                return False

    def stop(self):
        """
        Dừng TCP server.
        """
        self.running = False

        with self.lock:
            if self.client_sock is not None:
                try:
                    self.client_sock.close()
                except Exception:
                    pass
                self.client_sock = None

        if self.server_sock is not None:
            try:
                self.server_sock.close()
            except Exception:
                pass

        self.server_sock = None
