# app/services/csi_service.py
#
# CSI Management - binary storage.
#
# Luồng dữ liệu:
# Nexmon-Collection TCP server -> ethernet_manager TCP receiver -> ethernet_manager.csi_queue
# -> CsiService ghi raw_asus1/2/3.bin
#
# ESP32-Collection TCP server -> EspTcpClient -> uart_manager.csi_queue
# -> CsiService ghi raw_esp1/2/3.bin
#
# Binary record size:
# - ESP32: 144 bytes / packet
# - ASUS : 1044 bytes / packet

import struct
import threading
import time
from pathlib import Path

from app.services.ethernet_manager import ethernet_manager
from app.services.uart_manager import uart_manager
from app.core.time_utils import perf_now


CSI_WRITE_BATCH_SIZE = 500
FILE_FLUSH_INTERVAL_SEC = 2.0
BINARY_FILE_BUFFER_BYTES = 1024 * 1024
ESP_SEQ_MODULO = 4096

ESP_SUBCARRIER_COUNT = 64
ASUS_ANTENNA_COUNT = 4
ASUS_SUBCARRIER_COUNT = 64

# ESP32 144B:
# seq:uint16, timestamp:uint64, channel:uint16,
# agc:uint8, fft:uint8, noise:int8, rssi:int8,
# 64 * (q:int8 + i:int8)
ESP_HEADER_FMT = "<HQHBBbb"
ESP_HEADER_SIZE = struct.calcsize(ESP_HEADER_FMT)  # 16
ESP_RECORD_SIZE = ESP_HEADER_SIZE + ESP_SUBCARRIER_COUNT * 2  # 144

# ASUS 1044B:
# seq:uint16, timestamp:uint64, channel:uint16,
# agc0..3:uint8, rssi0..3:int8,
# 4 antenna * 64 subcarrier * 4 bytes packed decimal
ASUS_HEADER_FMT = "<HQHBBBBbbbb"
ASUS_HEADER_SIZE = struct.calcsize(ASUS_HEADER_FMT)  # 20
ASUS_RECORD_SIZE = ASUS_HEADER_SIZE + ASUS_ANTENNA_COUNT * ASUS_SUBCARRIER_COUNT * 4  # 1044


class CsiService:
    def __init__(self, session_dir, session_t0):
        self.session_dir = Path(session_dir)
        self.session_t0 = session_t0
        self.running = False

        self.threads = []
        self.files = {}
        self.last_flush_at = perf_now()

        self.nexmon_devices = ["asus1", "asus2", "asus3"]
        self.esp_devices = ["esp1", "esp2", "esp3"]

        # Đếm tiến trình seq ESP để action có thể chờ đủ packet theo từng ESP.
        # Không yêu cầu seq bắt đầu từ 0. Hỗ trợ seq quay vòng 0..4095.
        self._esp_seq_last: dict[str, int | None] = {
            device_id: None for device_id in self.esp_devices
        }
        self._esp_seq_progress: dict[str, int] = {
            device_id: 0 for device_id in self.esp_devices
        }
        self._esp_seq_lock = threading.Lock()

    # ============================================================
    # START / STOP
    # ============================================================
    def start_csi_collection(self):
        """
        Bắt đầu thu CSI cho session:
        1. Khóa không cho CSI mới vào queue trong lúc chuẩn bị.
        2. Xóa queue cũ.
        3. Reset bộ đếm seq ESP.
        4. Mở 6 file .bin.
        5. Start writer threads.
        6. Mở recording_enabled để CSI mới bắt đầu vào queue.
        """
        if self.running:
            print("CSI service already running.")
            return

        self.running = True

        self._set_recording_enabled(False)
        ethernet_manager.clear_csi_queue()
        uart_manager.clear_csi_queue()
        self._reset_esp_seq_progress()

        self._open_binary_files()
        self._start_threads()

        self._set_recording_enabled(True)
        print("CSI binary collection started.")

    def stop_csi_collection(self):
        """
        Dừng CSI collection:
        1. Khóa CSI mới không vào queue.
        2. self.running=False để writer chuẩn bị thoát.
        3. Writer vẫn drain hết packet đã có trong queue.
        4. Flush + close file .bin.
        """
        self._set_recording_enabled(False)
        self.running = False

        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)

        self.threads.clear()
        self._flush_binary_files(force=True)

        for f in self.files.values():
            try:
                f.close()
            except Exception:
                pass

        self.files.clear()
        print("CSI binary collection stopped.")

    def _set_recording_enabled(self, enabled: bool):
        for manager in (ethernet_manager, uart_manager):
            setter = getattr(manager, "set_recording_enabled", None)
            if callable(setter):
                setter(enabled)

    # ============================================================
    # ESP SEQ PROGRESS
    # ============================================================
    def _reset_esp_seq_progress(self):
        with self._esp_seq_lock:
            for device_id in self.esp_devices:
                self._esp_seq_last[device_id] = None
                self._esp_seq_progress[device_id] = 0

    def get_esp_seq_progress_total(self) -> int:
        """Giữ lại để tương thích code cũ: tổng bước seq của cả 3 ESP."""
        with self._esp_seq_lock:
            return sum(self._esp_seq_progress.values())

    def get_esp_seq_progress_snapshot(self) -> dict[str, int]:
        """
        Trả tiến trình seq riêng từng ESP.
        Dùng cho điều kiện: cả esp1, esp2, esp3 đều phải tăng >= 1000 bước.
        """
        with self._esp_seq_lock:
            return dict(self._esp_seq_progress)

    def _update_esp_seq_progress(self, device_id: str, seq_value):
        if device_id not in self.esp_devices:
            return

        try:
            seq = int(seq_value) % ESP_SEQ_MODULO
        except (TypeError, ValueError):
            return

        with self._esp_seq_lock:
            last = self._esp_seq_last.get(device_id)
            if last is None:
                self._esp_seq_last[device_id] = seq
                return

            # Hỗ trợ seq chạy 0..4095 rồi quay về 0.
            # 4095 -> 0 = 1; 4095 -> 2 = 3; 4092 -> 0 = 4.
            delta = (seq - last) % ESP_SEQ_MODULO
            if delta > 0:
                self._esp_seq_progress[device_id] += delta
                self._esp_seq_last[device_id] = seq

    # ============================================================
    # OPEN FILES / THREADS
    # ============================================================
    def _open_binary_files(self):
        for device_id in self.nexmon_devices:
            self._open_one_binary(device_id)

        for device_id in self.esp_devices:
            self._open_one_binary(device_id)

    def _open_one_binary(self, device_id: str):
        file_path = self.session_dir / f"raw_{device_id}.bin"
        f = open(file_path, "wb", buffering=BINARY_FILE_BUFFER_BYTES)
        self.files[device_id] = f

    def _start_threads(self):
        self._start_thread(target=self._write_nexmon_loop, name="nexmon-writer")
        self._start_thread(target=self._write_esp_loop, name="esp-writer")

    def _start_thread(self, target, name: str):
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        self.threads.append(thread)

    # ============================================================
    # WRITE LOOPS
    # ============================================================
    def _write_nexmon_loop(self):
        self._write_queue_loop(manager=ethernet_manager, source="nexmon")

    def _write_esp_loop(self):
        self._write_queue_loop(manager=uart_manager, source="esp")

    def _write_queue_loop(self, manager, source: str):
        """
        Writer dừng sau khi self.running=False và queue đã hết.
        Nhờ vậy khi STOP, packet đã vào queue vẫn được ghi nốt trước khi close file.
        """
        while self.running or not manager.csi_queue.empty():
            timeout = 0.1 if self.running else 0
            packet = manager.get_packet(timeout=timeout)

            if packet is None:
                self._flush_binary_files(force=False)
                if not self.running:
                    break
                continue

            self._write_packet(packet, source=source)

            for _ in range(CSI_WRITE_BATCH_SIZE - 1):
                packet = manager.get_packet(timeout=0)
                if packet is None:
                    break
                self._write_packet(packet, source=source)

            self._flush_binary_files(force=False)

    def _write_packet(self, packet: dict, source: str):
        device_id = packet.get("device_id")
        if not device_id:
            return

        device_id = str(device_id).strip()
        packet["device_id"] = device_id

        if device_id not in self.files:
            return
        
        # print(f"[CSI-SERVICE] Writing packet for device: {device_id}")

        if source == "esp" or device_id in self.esp_devices:
            record = self._pack_esp_record(packet)
            self.files[device_id].write(record)
            self._update_esp_seq_progress(device_id, packet.get("seq"))
            self._update_rate(uart_manager, device_id, packet.get("timestamp", packet.get("esp_timestamp_us")))
            return

        if source == "nexmon" or device_id in self.nexmon_devices:
            record = self._pack_asus_record(packet)
            self.files[device_id].write(record)
            self._update_rate(ethernet_manager, device_id, packet.get("timestamp"))
            return

    def _update_rate(self, manager, device_id: str, timestamp_us):
        updater = getattr(manager, "update_packet_stat", None)
        if not callable(updater):
            return

        try:
            updater(device_id, timestamp_us)
        except TypeError:
            # Tương thích manager cũ chỉ nhận update_packet_stat(device_id).
            updater(device_id)

    # ============================================================
    # PACK HELPERS
    # ============================================================
    def _to_int(self, value, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _u8(self, value) -> int:
        return max(0, min(255, self._to_int(value)))

    def _i8(self, value) -> int:
        return max(-128, min(127, self._to_int(value)))

    def _u16(self, value) -> int:
        return self._to_int(value) & 0xFFFF

    def _u64(self, value) -> int:
        return self._to_int(value) & 0xFFFFFFFFFFFFFFFF

    def _u32_raw(self, value) -> int:
        """
        Lưu đúng 4 byte của số thập phân CSI ASUS.
        Nếu value âm thì vẫn lưu dạng two's complement 32-bit bằng & 0xFFFFFFFF.
        """
        return self._to_int(value) & 0xFFFFFFFF

    def _normalize_csi_pairs(self, csi, pair_count: int = 64):
        """
        Chuẩn hóa ESP CSI về list các cặp [Q, I].
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

    # ============================================================
    # ESP32 BINARY: 144B
    # ============================================================
    def _pack_esp_record(self, packet: dict) -> bytes:
        radio = packet.get("radio") or {}

        seq = self._u16(packet.get("seq"))
        timestamp = self._u64(packet.get("timestamp", packet.get("esp_timestamp_us")))
        channel = self._u16(radio.get("channel", packet.get("channel", 0)))
        agc_gain = self._u8(radio.get("agc_gain", packet.get("agc_gain", 0)))
        fft_gain = self._u8(radio.get("fft_gain", packet.get("fft_gain", 0)))
        noise = self._i8(radio.get("noise_floor", packet.get("noise", packet.get("noise_floor", 0))))
        rssi = self._i8(radio.get("rssi", packet.get("rssi", 0)))

        header = struct.pack(
            ESP_HEADER_FMT,
            seq,
            timestamp,
            channel,
            agc_gain,
            fft_gain,
            noise,
            rssi,
        )

        csi = packet.get("csi", packet.get("csi_data"))
        pairs = self._normalize_csi_pairs(csi, pair_count=ESP_SUBCARRIER_COUNT)

        payload = bytearray()
        for idx in range(ESP_SUBCARRIER_COUNT):
            if idx < len(pairs):
                q, i = pairs[idx][0], pairs[idx][1]
                payload.extend(struct.pack("<bb", self._i8(q), self._i8(i)))
            else:
                payload.extend(b"\x00\x00")

        record = header + bytes(payload)
        if len(record) != ESP_RECORD_SIZE:
            raise RuntimeError(f"ESP record size sai: {len(record)} != {ESP_RECORD_SIZE}")
        return record

    # ============================================================
    # ASUS BINARY: 1044B
    # ============================================================
    def _pack_asus_record(self, packet: dict) -> bytes:
        """
        JSON ASUS mới:
        {
          "seq": 1,
          "timestamp": 1716280000123456,
          "bw": 20,
          "ch": 157,
          "agc": [0,0,0,0],
          "rssi": [2,3,4,5],
          "csi": {
            "c0": [123, 556, ...],  # 64 số thập phân, mỗi số là 4 byte CSI Q/I đã pack sẵn
            "c1": [...],
            "c2": [...],
            "c3": [...]
          }
        }

        Khi ghi binary: mỗi số decimal trong csi.c0..c3 được pack trực tiếp thành uint32 little-endian.
        """
        seq = self._u16(packet.get("seq"))
        timestamp = self._u64(packet.get("timestamp"))
        # channel = self._u16(packet.get("ch", packet.get("channel", packet.get("bw", 0))))
        channel = self._u16(packet.get("ch", packet.get("channel", 0)))

        agc = packet.get("agc", packet.get("agc_gain", []))
        rssi = packet.get("rssi", [])
        if not isinstance(agc, (list, tuple)):
            agc = []
        if not isinstance(rssi, (list, tuple)):
            rssi = []

        agc_values = [self._u8(agc[i] if i < len(agc) else 0) for i in range(4)]
        rssi_values = [self._i8(rssi[i] if i < len(rssi) else 0) for i in range(4)]

        header = struct.pack(
            ASUS_HEADER_FMT,
            seq,
            timestamp,
            channel,
            agc_values[0],
            agc_values[1],
            agc_values[2],
            agc_values[3],
            rssi_values[0],
            rssi_values[1],
            rssi_values[2],
            rssi_values[3],
        )

        csi = packet.get("csi") or {}
        payload = bytearray()

        for ant in range(ASUS_ANTENNA_COUNT):
            values = csi.get(f"c{ant}") or []
            if not isinstance(values, list):
                values = []

            for sub in range(ASUS_SUBCARRIER_COUNT):
                value = values[sub] if sub < len(values) else 0

                # Format mới: value là số thập phân đại diện cho đúng 4 byte Q/I.
                # Nếu lỡ nhận format cũ [[q,i], ...], vẫn pack được thành int16 q + int16 i.
                if isinstance(value, (list, tuple)) and len(value) >= 2:
                    payload.extend(struct.pack("<hh", self._to_int(value[0]), self._to_int(value[1])))
                else:
                    payload.extend(struct.pack("<I", self._u32_raw(value)))

        record = header + bytes(payload)
        if len(record) != ASUS_RECORD_SIZE:
            raise RuntimeError(f"ASUS record size sai: {len(record)} != {ASUS_RECORD_SIZE}")
        return record

    # ============================================================
    # FLUSH
    # ============================================================
    def _flush_binary_files(self, force: bool = False):
        now = perf_now()
        if not force and now - self.last_flush_at < FILE_FLUSH_INTERVAL_SEC:
            return

        for f in self.files.values():
            try:
                f.flush()
            except Exception:
                pass

        self.last_flush_at = now
