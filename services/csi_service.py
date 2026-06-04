# app/services/csi_service.py
#
# CSI Management.
#
# Luồng dữ liệu:
# Nexmon-Collection TCP server -> ethernet_manager TCP receiver -> ethernet_manager.csi_queue
# -> CsiService ghi raw_asus1/2/3.csv
#
# ESP32-Collection TCP server -> EspTcpClient -> uart_manager.csi_queue
# -> CsiService ghi raw_esp1/2/3.csv
#
# Quy ước:
# - Collection có thể gửi device_id là MAC thật.
# - Adapter TCP map MAC -> asus1/asus2/asus3 hoặc esp1/esp2/esp3.
# - CSV vẫn giữ tên file cũ raw_asus1.csv... raw_esp3.csv.
# - CSI Q/I được tách thành từng cột riêng để mở Excel dễ xử lý.

import csv
import threading
from pathlib import Path

from app.services.ethernet_manager import ethernet_manager
from app.services.uart_manager import uart_manager
from app.core.time_utils import perf_now


# Ghi theo batch để giảm overhead khi CSI tốc độ cao.
CSI_WRITE_BATCH_SIZE = 500
CSV_FLUSH_INTERVAL_SEC = 2.0
CSV_FILE_BUFFER_BYTES = 1024 * 1024

# ESP seq chạy 0 -> 4095 rồi quay lại 0.
ESP_SEQ_MODULO = 4096


class CsiService:
    def __init__(self, session_dir, session_t0):
        self.session_dir = Path(session_dir)
        self.session_t0 = session_t0
        self.running = False

        self.threads = []
        self.files = {}
        self.writers = {}
        self.last_flush_at = perf_now()

        # Sau khi adapter map MAC -> alias, các tầng sau vẫn dùng ID cũ.
        self.nexmon_devices = ["asus1", "asus2", "asus3"]
        self.esp_devices = ["esp1", "esp2", "esp3"]

        # Đếm tiến trình seq ESP để action có thể chờ đủ số bước seq.
        # Không yêu cầu seq bắt đầu từ 0. Hỗ trợ seq quay vòng 0..4095.
        self._esp_seq_last: dict[str, int | None] = {
            device_id: None for device_id in self.esp_devices
        }
        self._esp_seq_progress: dict[str, int] = {
            device_id: 0 for device_id in self.esp_devices
        }
        self._esp_seq_lock = threading.Lock()

    def start_csi_collection(self):
        """
        Hàm được recording_service.py gọi khi bắt đầu session.

        Nhiệm vụ:
        1. Tạm khóa việc đưa CSI mới vào queue.
        2. Xóa queue cũ.
        3. Reset bộ đếm seq ESP.
        4. Mở file CSV.
        5. Start thread writer lấy packet từ queue ghi file.
        6. Mở khóa recording_enabled để packet mới bắt đầu vào queue.

        Lưu ý:
        - Nexmon TCP client do ethernet_manager giữ sẵn sau khi Web bấm configure_tcp.
        - ESP TCP client do uart_manager giữ sẵn sau khi Web bấm Refresh/Kết nối COM.
        """
        if self.running:
            print("CSI service already running.")
            return

        self.running = True

        # Chặn packet mới vào queue trong lúc chuẩn bị session.
        ethernet_manager.set_recording_enabled(False)
        uart_manager.set_recording_enabled(False)

        # Xóa dữ liệu tồn từ trước session nếu có.
        ethernet_manager.clear_csi_queue()
        uart_manager.clear_csi_queue()
        self._reset_esp_seq_progress()

        self._open_csv_files()
        self._start_threads()

        # Sau khi writer/file đã sẵn sàng, mới cho packet mới vào queue.
        ethernet_manager.set_recording_enabled(True)
        uart_manager.set_recording_enabled(True)

        print("CSI collection started.")

    # ============================================================
    # ESP seq progress
    # ============================================================
    def _reset_esp_seq_progress(self):
        with self._esp_seq_lock:
            for device_id in self.esp_devices:
                self._esp_seq_last[device_id] = None
                self._esp_seq_progress[device_id] = 0

    def get_esp_seq_progress_total(self) -> int:
        """
        Trả tổng số bước seq của 3 ESP. Giữ lại để tương thích code cũ.
        """
        with self._esp_seq_lock:
            return sum(self._esp_seq_progress.values())

    def get_esp_seq_progress_snapshot(self) -> dict[str, int]:
        """
        Trả tiến trình seq riêng từng ESP.
        Dùng khi muốn cả esp1, esp2, esp3 đều phải đủ packet_target.
        """
        with self._esp_seq_lock:
            return dict(self._esp_seq_progress)

    def _update_esp_seq_progress(self, device_id: str, seq_value):
        if device_id not in self.esp_devices:
            return

        try:
            seq = int(seq_value)
        except (TypeError, ValueError):
            return

        seq %= ESP_SEQ_MODULO

        with self._esp_seq_lock:
            last = self._esp_seq_last.get(device_id)

            if last is None:
                self._esp_seq_last[device_id] = seq
                return

            # Hỗ trợ seq chạy 0..4095 rồi quay về 0.
            # Ví dụ:
            # 20 -> 25      delta = 5
            # 4095 -> 0     delta = 1
            # 4095 -> 2     delta = 3
            # 4092 -> 0     delta = 4
            # 100 -> 100    delta = 0
            delta = (seq - last) % ESP_SEQ_MODULO

            if delta > 0:
                self._esp_seq_progress[device_id] += delta
                self._esp_seq_last[device_id] = seq

    # ============================================================
    # CSV open/header
    # ============================================================
    def _open_csv_files(self):
        """Mở CSV cho 6 thiết bị cố định."""
        for device_id in self.nexmon_devices:
            self._open_one_csv(device_id)

        for device_id in self.esp_devices:
            self._open_one_csv(device_id)

    def _csv_header_for_device(self, device_id: str):    # Trả header riêng cho ESP và ASUS. Nếu không match mapping thì trả lại device_id đã strip để dễ debug.
        """
        Header riêng cho ESP và ASUS.

        ESP:
        device_id,seq,timestamp,rssi,channel,agc_gain,fft_gain,noise_floor,
        csi_Q_0,csi_I_0,csi_Q_1,csi_I_1,...,csi_Q_63,csi_I_63

        ASUS/Nexmon:
        device_id,seq,timestamp,bw,agc,rssi,
        csi0_Q_0,csi0_I_0,...,csi0_Q_63,csi0_I_63,
        csi1_Q_0,csi1_I_0,...,
        csi2_Q_0,csi2_I_0,...,
        csi3_Q_0,csi3_I_0,...

        Lưu ý:
        - Tên antenna trong CSV bắt đầu từ 0: csi0, csi1, csi2, csi3.
        - Tên subcarrier trong CSV bắt đầu từ 0.
        - JSON ASUS có key c0,c1,c2,c3; khi ghi CSV map c0 -> csi0, c1 -> csi1...
        """
        if device_id in self.esp_devices:
            csi_cols = []
            for idx in range(64):
                csi_cols.extend([f"csi_Q_{idx}", f"csi_I_{idx}"])

            return [
                "device_id",
                "seq",
                "timestamp",
                "rssi",
                "channel",
                "agc_gain",
                "fft_gain",
                "noise_floor",
                *csi_cols,
            ]

        if device_id in self.nexmon_devices:
            csi_cols = []
            for ant in range(4):
                for idx in range(64):
                    csi_cols.extend([f"csi{ant}_Q_{idx}", f"csi{ant}_I_{idx}"])

            return [
                "device_id",
                "seq",
                "timestamp",
                "bw",
                "agc",
                "rssi",
                *csi_cols,
            ]

        return ["raw"]

    def _open_one_csv(self, device_id: str):
        """Tạo file raw_<device_id>.csv."""
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

    # ============================================================
    # Threads / writer loops
    # ============================================================
    def _start_threads(self):
        """
        Start các thread writer.
        TCP receiver chạy trong ethernet_manager/uart_manager, không tạo ở đây nữa.
        """
        self._start_thread(target=self._write_nexmon_loop, name="nexmon-writer")
        self._start_thread(target=self._write_esp_loop, name="esp-writer")

    def _start_thread(self, target, name: str):
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        self.threads.append(thread)

    def _write_nexmon_loop(self):
        """Lấy packet từ ethernet_manager.csi_queue và ghi file raw_asus*.csv."""
        self._write_queue_loop(manager=ethernet_manager, source="nexmon")

    def _write_esp_loop(self):
        """Lấy packet từ uart_manager.csi_queue và ghi file raw_esp*.csv."""
        self._write_queue_loop(manager=uart_manager, source="esp")

    def _write_queue_loop(self, manager, source: str):
        """
        Ghi theo batch để giảm overhead khi tốc độ packet cao.
        Khi stop session, recording_enabled đã tắt nên không có packet mới vào queue;
        loop vẫn drain hết packet còn lại trước khi thoát để tránh mất đoạn cuối.
        """
        while self.running or not manager.csi_queue.empty():
            timeout = 0.1 if self.running else 0
            packet = manager.get_packet(timeout=timeout)

            if packet is None:
                self._flush_csv_files(force=False)
                if not self.running:
                    break
                continue

            self._write_packet(packet, source=source)

            # Drain nhanh các packet đang có sẵn trong queue.
            for _ in range(CSI_WRITE_BATCH_SIZE - 1):
                packet = manager.get_packet(timeout=0)
                if packet is None:
                    break
                self._write_packet(packet, source=source)

            self._flush_csv_files(force=False)

    # ============================================================
    # Data normalization helpers
    # ============================================================
    def _list_cell_space(self, value) -> str:
        """
        List trong một ô CSV, cách nhau bằng khoảng trắng để xem Excel dễ hơn.
        Ví dụ [0,0,0,0] -> "0 0 0 0".
        """
        if value is None:
            return ""
        if isinstance(value, (list, tuple)):
            return " ".join(str(x) for x in value)
        return str(value)

    def _normalize_csi_pairs(self, csi, pair_count: int = 64):
        """
        Chuẩn hóa CSI về list các cặp [Q, I].
        Hỗ trợ:
        - [[q0,i0], [q1,i1], ...]
        - [q0, i0, q1, i1, ...]
        """
        if not isinstance(csi, list) or not csi:
            return []

        if isinstance(csi[0], (list, tuple)):
            pairs = []
            for pair in csi[:pair_count]:
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    pairs.append([pair[0], pair[1]])
            return pairs

        pairs = []
        flat_limit = min(len(csi), pair_count * 2)
        for i in range(0, flat_limit, 2):
            if i + 1 < len(csi):
                pairs.append([csi[i], csi[i + 1]])
        return pairs

    def _flatten_qi_columns(self, pairs, pair_count: int = 64): # hàm này đảm bảo rằng dù cặp Q/I có đủ hay không thì số cột vẫn luôn cố định, thiếu cặp nào thì điền rỗng. Ví dụ nếu chỉ có 3 cặp thì vẫn trả về list 128 phần tử, trong đó 6 phần đầu là Q0,I0,Q1,I1,Q2,I2 rồi đến hết là "".
        """
        Đổi list [[Q,I],...] thành list [Q0,I0,Q1,I1,...].
        Nếu thiếu subcarrier thì ghi rỗng để số cột luôn cố định.
        """
        cells = []
        for idx in range(pair_count):
            if idx < len(pairs) and isinstance(pairs[idx], (list, tuple)) and len(pairs[idx]) >= 2:
                cells.extend([pairs[idx][0], pairs[idx][1]])
            else:
                cells.extend(["", ""])
        return cells

    # ============================================================
    # Packet writing
    # ============================================================
    def _write_packet(self, packet: dict, source: str):  
        """Ghi 1 packet vào file tương ứng theo device_id."""
        device_id = packet.get("device_id")
        if not device_id:
            return

        device_id = str(device_id).strip()
        packet["device_id"] = device_id

        if device_id not in self.writers:
            return

        if source == "esp" or device_id in self.esp_devices:
            self._write_esp_packet(packet)
            self._update_esp_seq_progress(device_id, packet.get("seq"))
            uart_manager.update_packet_stat(
                device_id,
                packet.get("timestamp", packet.get("esp_timestamp_us")),
            )
            return

        if source == "nexmon" or device_id in self.nexmon_devices:
            self._write_asus_packet(packet)
            ethernet_manager.update_packet_stat(device_id, packet.get("timestamp"))
            return
 
    def _write_esp_packet(self, packet: dict): # CSV ESP columns:
        """
        CSV ESP columns:
        device_id,seq,timestamp,rssi,channel,agc_gain,fft_gain,noise_floor,
        csi_Q_0,csi_I_0,...,csi_Q_63,csi_I_63
        """
        device_id = packet.get("device_id")
        radio = packet.get("radio") or {}
        csi = packet.get("csi", packet.get("csi_data"))
        csi_pairs = self._normalize_csi_pairs(csi, pair_count=64)
        csi_cells = self._flatten_qi_columns(csi_pairs, pair_count=64)

        self.writers[device_id].writerow([
            device_id,
            packet.get("seq"),
            packet.get("timestamp", packet.get("esp_timestamp_us")),
            radio.get("rssi"),
            radio.get("channel"),
            radio.get("agc_gain"),
            radio.get("fft_gain"),
            radio.get("noise_floor"),
            *csi_cells,
        ])

    def _write_asus_packet(self, packet: dict): # CSV ASUS columns:
        """
        CSV ASUS columns:
        device_id,seq,timestamp,bw,agc,rssi,
        csi0_Q_0,csi0_I_0,...,csi3_Q_63,csi3_I_63

        JSON ASUS vẫn dùng key c0,c1,c2,c3. Khi ghi CSV:
        c0 -> csi0, c1 -> csi1, c2 -> csi2, c3 -> csi3.
        """
        device_id = packet.get("device_id")
        csi = packet.get("csi") or {}
        csi_cells = []

        for ant_index in range(4):
            pairs = csi.get(f"c{ant_index}") or []
            pairs = self._normalize_csi_pairs(pairs, pair_count=64)
            csi_cells.extend(self._flatten_qi_columns(pairs, pair_count=64))

        self.writers[device_id].writerow([
            device_id,
            packet.get("seq"),
            packet.get("timestamp"),
            packet.get("bw"),
            self._list_cell_space(packet.get("agc")),
            self._list_cell_space(packet.get("rssi")),
            *csi_cells,
        ])

    # ============================================================
    # Flush/stop
    # ============================================================
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
        Dừng ghi CSI của session hiện tại:
        1. Tắt recording_enabled để packet mới không vào queue nữa.
        2. Cho writer thread drain hết queue còn lại.
        3. Flush và đóng CSV.

        Không đóng TCP Nexmon/ESP ở đây; TCP được manager giữ sẵn giống nhau.
        """
        ethernet_manager.set_recording_enabled(False)
        uart_manager.set_recording_enabled(False)

        self.running = False

        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)

        self.threads.clear()

        self._flush_csv_files(force=True)

        for f in self.files.values():
            try:
                f.close()
            except Exception:
                pass

        self.files.clear()
        self.writers.clear()

        print("CSI collection stopped.")
