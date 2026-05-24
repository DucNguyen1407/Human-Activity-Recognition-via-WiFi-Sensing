# app/core/time_utils.py
#
# Chuẩn thời gian dùng trong toàn bộ hệ thống:
# - timestamp lưu ra file / gửi qua TCP: Unix timestamp đơn vị micro giây (int)
# - perf_now(): chỉ dùng nội bộ để đo khoảng thời gian chạy, không ghi như timestamp tuyệt đối

from datetime import datetime, timezone
import time
import uuid


def unix_now_us() -> int:
    """
    Unix timestamp hiện tại đơn vị micro giây.

    Ví dụ: 1716023475123456
    """
    return time.time_ns() // 1_000


def perf_now() -> float:
    """
    Bộ đếm hiệu năng đơn vị giây, chỉ dùng để đo elapsed/duration nội bộ.
    Không dùng giá trị này làm timestamp tuyệt đối.
    """
    return time.perf_counter()


def utc_now():
    """Giữ lại để tương thích code cũ nếu còn import, không dùng cho dữ liệu mới."""
    return datetime.now(timezone.utc)


def utc_now_iso():
    """Giữ lại để tương thích code cũ nếu còn import, không dùng cho dữ liệu mới."""
    return utc_now().isoformat()


def new_session_id():
    """Tạo mã phiên ngẫu nhiên nếu sau này cần."""
    return uuid.uuid4().hex
