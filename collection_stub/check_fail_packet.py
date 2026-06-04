def calculate_packet_loss(
    seq_start,
    seq_end,
    timestamp_start,
    timestamp_end,
    received_count,
    packet_rate=200,
    seq_mod=4096
):
    """
    timestamp_start, timestamp_end: đơn vị micro giây
    seq_start, seq_end: seq từ 0 đến 4095
    received_count: tổng số gói thực tế nhận được
    packet_rate: 200 gói / giây
    """

    # 1. Tính thời gian chạy theo giây
    duration_sec = (timestamp_end - timestamp_start) / 1_000_000

    if duration_sec < 0:
        raise ValueError("timestamp_end phải lớn hơn timestamp_start")

    # 2. Seq chênh lệch theo vòng 0 -> 4095 -> 0
    seq_diff_mod = (seq_end - seq_start) % seq_mod

    # 3. Số bước seq dự kiến theo thời gian
    expected_steps_by_time = round(duration_sec * packet_rate)

    # 4. Ước lượng số lần seq quay vòng
    wraps = round((expected_steps_by_time - seq_diff_mod) / seq_mod)
    wraps = max(0, wraps)

    # 5. Tổng số bước seq thật
    real_seq_steps = seq_diff_mod + wraps * seq_mod

    # 6. Số gói đáng lẽ phải có
    # +1 vì tính cả gói đầu và gói cuối
    expected_count = real_seq_steps + 1

    # 7. Số gói mất
    lost_packets = expected_count - received_count
    lost_packets = max(0, lost_packets)

    # 8. Tỉ lệ lỗi bản tin
    loss_rate_percent = 0
    if expected_count > 0:
        loss_rate_percent = lost_packets / expected_count * 100

    return {
        "duration_sec": duration_sec,
        "seq_start": seq_start,
        "seq_end": seq_end,
        "seq_diff_mod": seq_diff_mod,
        "wraps": wraps,
        "expected_count": expected_count,
        "received_count": received_count,
        "lost_packets": lost_packets,
        "loss_rate_percent": loss_rate_percent,
    }
# 1
result1 = calculate_packet_loss(
    seq_start=2010,
    seq_end=2431,
    timestamp_start=1779878569063540,
    timestamp_end=1779878612065210,
    received_count=8608
)

result2 = calculate_packet_loss(
    seq_start=2010,
    seq_end=2431,
    timestamp_start=1779878569130130,
    timestamp_end=1779878612131610,
    received_count=8607
)
result3 = calculate_packet_loss(
    seq_start=2010,
    seq_end=2431,
    timestamp_start=1779878569051120,
    timestamp_end=1779878612052690,
    received_count=8607
)

print(result1)
print(result2)
print(result3)