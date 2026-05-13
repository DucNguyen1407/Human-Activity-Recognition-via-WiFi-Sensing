# CSI service mới là nơi:
# - đọc queue
# - lọc theo device_id
# - ghi 6 file raw

import threading
from app.adapters.csi_eth_adapter import CsiEthAdapter


class CsiService:
    def __init__(self, session_dir, session_t0):
        self.csi_adapter = CsiEthAdapter(session_dir, session_t0)
        self.csi_thread = None

    def start_csi_collection(self):
        self.csi_thread = threading.Thread(
            target=self.csi_adapter.start_recording,
            daemon=True
        )
        self.csi_thread.start()
        print("CSI collection started.")

    def stop_csi_collection(self):
        self.csi_adapter.stop()

        if self.csi_thread and self.csi_thread.is_alive():
            self.csi_thread.join(timeout=5)

        print("CSI collection stopped.")