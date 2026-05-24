# app/services/csi_service.py
#
# CSI Management.
#
# Luồng dữ liệu:
# Nexmon-Collection TCP server -> NexmonTcpClient -> ethernet_manager.csi_queue
# -> CsiService ghi raw_asus1/2/3.csv
#
# ESP32-Collection TCP server -> EspTcpClient -> uart_manager.csi_queue
# -> CsiService ghi raw_esp1/2/3.csv
#
# Quy ước mới:
# - Collection có thể gửi device_id là MAC thật.
# - Adapter TCP map MAC -> asus1/asus2/asus3 hoặc esp1/esp2/esp3.
# - CSV vẫn giữ tên file cũ raw_asus1.csv... raw_esp3.csv.
# - CSV ESP và ASUS có header riêng, chỉ lưu field có trong JSON nguồn.

import csv
import json
import threading
import time
from pathlib import Path

from app.adapters.nexmon_tcp_client import NexmonTcpClient
from app.services.ethernet_manager import ethernet_manager
from app.services.uart_manager import uart_manager

from app.core.time_utils import perf_now


# Ghi theo batch để giảm overhead khi CSI tốc độ cao.
CSI_WRITE_BATCH_SIZE = 500
CSV_FLUSH_INTERVAL_SEC = 2.0
CSV_FILE_BUFFER_BYTES = 1024 * 1024


class CsiService:
    def __init__(self, session_dir, session_t0):
        self.session_dir = Path(session_dir)
        self.session_t0 = session_t0

        self.running = False

        self.nexmon_client = None
        self.threads = []

        self.files = {}
        self.writers = {}
        self.last_flush_at = perf_now()

        # Sau khi adapter map MAC -> alias, các tầng sau vẫn dùng ID cũ.
        self.nexmon_devices = ["asus1", "asus2", "asus3"]
        self.esp_devices = ["esp1", "esp2", "esp3"]

    def start_csi_collection(self):
        """
        Hàm được recording_service.py gọi khi bắt đầu session.

        Nhiệm vụ:
        1. Xóa queue cũ
        2. Mở file CSV
        3. Connect tới Nexmon TCP server nếu có
        4. ESP TCP đã do uart_manager giữ sẵn để lấy COM/status/CSI
        5. Start thread lấy packet từ queue ghi file
        """
        if self.running:
            print("CSI service already running.")
            return

        self.running = True

        # Xóa dữ liệu tồn từ session trước nếu có.
        ethernet_manager.clear_csi_queue()
        uart_manager.clear_csi_queue()

        self._open_csv_files()

        self._connect_clients()

        self._start_threads()

        print("CSI collection started.")

    def _open_csv_files(self):
        """
        Mở CSV cho 6 thiết bị cố định.
        """
        for device_id in self.nexmon_devices:
            self._open_one_csv(device_id)

        for device_id in self.esp_devices:
            self._open_one_csv(device_id)

    def _csv_header_for_device(self, device_id: str):
        """
        Header riêng cho ESP và ASUS.

        ESP JSON:
        {
          "type":"csi_data",
          "device_id":"esp1",
          "seq":123,
          "timestamp":1716023475123456,
          "radio":{"rssi":-45,"channel":6,"agc_gain":1,"fft_gain":2,"noise_floor":-95},
          "csi":[[12,-3],[5,8],...]
        }

        ASUS JSON:
        {
          "device_id":"asus1",
          "seq":1,
          "timestamp":1716280000123456,
          "bw":20,
          "agc":[0,0,0,0],
          "rssi":[2,3,4,5],
          "csi":{"c0":[...],"c1":[...],"c2":[...],"c3":[...]}
        }
        """
        if device_id in self.esp_devices:
            return [
                "device_id",
                "seq",
                "timestamp",
                "rssi",
                "channel",
                "agc_gain",
                "fft_gain",
                "noise_floor",
                "csi",
            ]

        if device_id in self.nexmon_devices:
            return [
                "device_id",
                "seq",
                "timestamp",
                "bw",
                "agc",
                "rssi",
                "csi_0",
                "csi_1",
                "csi_2",
                "csi_3",
            ]

        return ["raw"]

    def _open_one_csv(self, device_id: str):
        """
        Tạo file raw_<device_id>.csv.
        """
        file_path = self.session_dir / f"raw_{device_id}.csv"

        # Buffer lớn giúp giảm số lần ghi xuống ổ đĩa khi CSI tốc độ cao.
        f = open(
            file_path,
            "w",
            newline="",
            encoding="utf-8",
            buffering=CSV_FILE_BUFFER_BYTES,
        )
        writer = csv.writer(f)
        writer.writerow(self._csv_header_for_device(device_id))

        self.files[device_id] = f
        self.writers[device_id] = writer

    def _connect_clients(self):
        """
        Kết nối tới Nexmon-Collection và ESP32-Collection.
        """
        # Nexmon TCP client
        nexmon_host = ethernet_manager.queue["host"]
        nexmon_port = ethernet_manager.queue["port"]

        self.nexmon_client = NexmonTcpClient(
            host=nexmon_host,
            port=nexmon_port,
        )

        try:
            self.nexmon_client.connect()
            ethernet_manager.set_queue_connected(True)
            print(f"Nexmon TCP client connected to {nexmon_host}:{nexmon_port}")

        except Exception as e:
            ethernet_manager.set_queue_connected(False)
            self.nexmon_client = None
            print(f"Cannot connect Nexmon TCP server {nexmon_host}:{nexmon_port}: {e}")

        # ESP TCP client được UartManager giữ kết nối dùng chung.
        # CsiService không tạo thêm TCP client thứ hai, tránh làm Collection bị chiếm socket.
        try:
            if uart_manager.queue.get("connected"):
                print("ESP TCP client already connected by uart_manager")
            else:
                print("ESP TCP client is not connected yet. Web cần bấm Refresh COM/Kết nối Collection trước.")
        except Exception as e:
            print(f"Cannot check ESP TCP connection: {e}")

    def _start_threads(self):
        """
        Start các thread:
        - TCP receiver thread
        - Queue writer thread
        """
        if self.nexmon_client is not None:
            self._start_thread(
                target=self._receive_nexmon_loop,
                name="nexmon-receiver",
            )

        # Writer thread vẫn chạy kể cả client chưa connect,
        # vì có thể sau này thêm reconnect.
        self._start_thread(
            target=self._write_nexmon_loop,
            name="nexmon-writer",
        )

        self._start_thread(
            target=self._write_esp_loop,
            name="esp-writer",
        )

    def _start_thread(self, target, name: str):
        thread = threading.Thread(
            target=target,
            name=name,
            daemon=True,
        )
        thread.start()
        self.threads.append(thread)

    def _receive_nexmon_loop(self):
        """
        Đọc packet từ TCP client Nexmon rồi đưa vào ethernet_manager.csi_queue.
        """
        while self.running and self.nexmon_client is not None:
            packet = self.nexmon_client.read_packet()

            if packet is None:
                time.sleep(0.001)
                continue

            device_id = packet.get("device_id")

            # Adapter đã map MAC -> asus1/asus2/asus3.
            if device_id not in self.nexmon_devices:
                continue

            ethernet_manager.put_packet(packet)

    def _write_nexmon_loop(self):
        """
        Lấy packet từ ethernet_manager.csi_queue và ghi file raw_asus*.csv.
        """
        self._write_queue_loop(
            manager=ethernet_manager,
            source="nexmon",
        )

    def _write_esp_loop(self):
        """
        Lấy packet từ uart_manager.csi_queue và ghi file raw_esp*.csv.
        """
        self._write_queue_loop(
            manager=uart_manager,
            source="esp",
        )

    def _write_queue_loop(self, manager, source: str):
        """
        Ghi theo batch để giảm overhead khi tốc độ packet cao.
        """
        while self.running:
            packet = manager.get_packet(timeout=0.1)

            if packet is None:
                self._flush_csv_files(force=False)
                continue

            self._write_packet(packet, source=source)

            # Drain nhanh các packet đang có sẵn trong queue.
            for _ in range(CSI_WRITE_BATCH_SIZE - 1):
                packet = manager.get_packet(timeout=0)
                if packet is None:
                    break
                self._write_packet(packet, source=source)

            self._flush_csv_files(force=False)

    def _array_cell(self, value):
        """
        Giữ mảng/list dạng dễ đọc trong 1 cell CSV.

        Ví dụ:
        [0, 0, 0, 0] -> "[0, 0, 0, 0]"
        [[12, -3], [5, 8]] -> "[[12, -3], [5, 8]]"

        Vì delimiter CSV là dấu phẩy, csv.writer sẽ tự thêm dấu nháy kép
        quanh các cell có dấu phẩy. Đây là đúng chuẩn CSV.
        """
        if value is None:
            return ""

        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)

        return value

    def _write_packet(self, packet: dict, source: str):
        """
        Ghi 1 packet vào file tương ứng theo device_id.
        """
        device_id = packet.get("device_id")

        if not device_id:
            return

        device_id = str(device_id).strip()
        packet["device_id"] = device_id

        # Chỉ ghi nếu file writer tương ứng đã được mở.
        if device_id not in self.writers:
            return

        if source == "esp" or device_id in self.esp_devices:
            self._write_esp_packet(packet)
            uart_manager.update_packet_stat(device_id)
            return

        if source == "nexmon" or device_id in self.nexmon_devices:
            self._write_asus_packet(packet)
            ethernet_manager.update_packet_stat(device_id)
            return

    def _write_esp_packet(self, packet: dict):
        """
        CSV ESP columns:
        device_id,seq,timestamp,rssi,channel,agc_gain,fft_gain,noise_floor,csi
        """
        device_id = packet.get("device_id")
        radio = packet.get("radio") or {}

        self.writers[device_id].writerow([
            device_id,
            packet.get("seq"),
            packet.get("timestamp", packet.get("esp_timestamp_us")),
            radio.get("rssi"),
            radio.get("channel"),
            radio.get("agc_gain"),
            radio.get("fft_gain"),
            radio.get("noise_floor"),
            self._array_cell(packet.get("csi", packet.get("csi_data"))),
        ])

    def _write_asus_packet(self, packet: dict):
        """
        CSV ASUS columns:
        device_id,seq,timestamp,bw,agc,rssi,csi_0,csi_1,csi_2,csi_3
        """
        device_id = packet.get("device_id")
        csi = packet.get("csi") or {}

        self.writers[device_id].writerow([
            device_id,
            packet.get("seq"),
            packet.get("timestamp"),
            packet.get("bw"),
            self._array_cell(packet.get("agc")),
            self._array_cell(packet.get("rssi")),
            self._array_cell(csi.get("c0")),
            self._array_cell(csi.get("c1")),
            self._array_cell(csi.get("c2")),
            self._array_cell(csi.get("c3")),
        ])

    def _flush_csv_files(self, force: bool = False):
        now = perf_now()

        if not force and now - self.last_flush_at < CSV_FLUSH_INTERVAL_SEC:
            return

        for f in self.files.values():
            try:
                f.flush()
            except Exception:
                pass

        self.last_flush_at = now

    def stop_csi_collection(self):
        """
        Dừng CSI collection:
        - dừng thread loop
        - đóng TCP client
        - đóng file CSV
        """
        self.running = False

        # Đóng socket trước để read_packet thoát nhanh.
        if self.nexmon_client:
            self.nexmon_client.close()

        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=2)

        self.threads.clear()

        ethernet_manager.set_queue_connected(False)

        self._flush_csv_files(force=True)

        for f in self.files.values():
            try:
                f.close()
            except Exception:
                pass

        self.files.clear()
        self.writers.clear()

        print("CSI collection stopped.")
