from datetime import datetime, timezone
import time
import uuid

def utc_now():
    """
    Lấy thời gian hiện tại theo UTC (có thông tin múi giờ).
    
    Trả về:
        datetime: Đối tượng datetime UTC hiện tại.
    """
    return datetime.now(timezone.utc)

def utc_now_iso():
    """
    Lấy thời gian hiện tại theo UTC ở định dạng ISO 8601.
    
    Trả về:
        str: Chuỗi thời gian ISO 8601.
    """
    return utc_now().isoformat()

def perf_now():
    """
    Lấy giá trị bộ đếm hiệu năng có độ phân giải cao.
    Dùng để đo thời gian chạy của một đoạn code (tính bằng giây).
    
    Trả về:
        float: Giá trị bộ đếm hiệu năng.
    """
    return time.perf_counter()

def new_session_id():
    """
    Tạo một mã phiên (session ID) duy nhất dạng chuỗi hex 32 ký tự.
    
    Trả về:
        str: Mã phiên duy nhất.
    """
    return uuid.uuid4().hex