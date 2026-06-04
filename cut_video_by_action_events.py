import csv
import re
import sys
from pathlib import Path

import cv2


def safe_name(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r"[^\w\-]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def load_video_index(video_index_path: Path):
    rows = []

    with open(video_index_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                rows.append({
                    "frame_no": int(row["frame_no"]),
                    "elapsed_us": int(row["elapsed_us"]),
                })
            except Exception:
                continue

    rows.sort(key=lambda x: x["elapsed_us"])
    return rows


def find_frame_range(video_index, start_elapsed_us: int, end_elapsed_us: int):
    matched = [
        item for item in video_index
        if start_elapsed_us <= item["elapsed_us"] <= end_elapsed_us
    ]

    if not matched:
        return None, None

    first_frame = matched[0]["frame_no"]
    last_frame = matched[-1]["frame_no"]

    return first_frame, last_frame


def cut_video_by_frame_range(
    video_path: Path,
    output_path: Path,
    first_frame: int,
    last_frame: int,
):
    cap = cv2.VideoCapture(str(video_path))

    if not cap.isOpened():
        raise RuntimeError(f"Không mở được video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    if fps <= 0:
        fps = 30

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(
        str(output_path),
        fourcc,
        fps,
        (width, height),
    )

    if not writer.isOpened():
        cap.release()
        raise RuntimeError(f"Không tạo được file output: {output_path}")

    # OpenCV đếm frame từ 0, còn video_index.csv frame_no đếm từ 1
    cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, first_frame - 1))

    current_frame = first_frame

    while current_frame <= last_frame:
        ok, frame = cap.read()

        if not ok or frame is None:
            break

        writer.write(frame)
        current_frame += 1

    writer.release()
    cap.release()


def cut_session_video(session_dir: str | Path):
    session_dir = Path(session_dir)

    video_path = session_dir / "video.mp4"
    video_index_path = session_dir / "video_index.csv"
    action_events_path = session_dir / "action_events.csv"
    segments_dir = session_dir / "segments"

    if not video_path.exists():
        raise FileNotFoundError(f"Không thấy video.mp4: {video_path}")

    if not video_index_path.exists():
        raise FileNotFoundError(f"Không thấy video_index.csv: {video_index_path}")

    if not action_events_path.exists():
        raise FileNotFoundError(f"Không thấy action_events.csv: {action_events_path}")

    segments_dir.mkdir(parents=True, exist_ok=True)

    video_index = load_video_index(video_index_path)

    if not video_index:
        raise RuntimeError("video_index.csv rỗng hoặc không đọc được dữ liệu frame")

    with open(action_events_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            action_index = int(row["action_index"])
            repeat_index = int(row["repeat_index"])
            action_name = row["action_name"]

            start_elapsed_us = int(row["start_elapsed_us"])
            end_elapsed_us = int(row["end_elapsed_us"])

            first_frame, last_frame = find_frame_range(
                video_index,
                start_elapsed_us,
                end_elapsed_us,
            )

            if first_frame is None or last_frame is None:
                print(
                    f"[SKIP] action={action_index}, "
                    f"{action_name}: không tìm thấy frame phù hợp"
                )
                continue

            output_name = (
                f"action_{action_index:03d}_"
                f"repeat_{repeat_index}_"
                f"{safe_name(action_name)}.mp4"
            )

            output_path = segments_dir / output_name

            print(
                f"[CUT] {output_name} | "
                f"elapsed={start_elapsed_us}->{end_elapsed_us} | "
                f"frame={first_frame}->{last_frame}"
            )

            cut_video_by_frame_range(
                video_path=video_path,
                output_path=output_path,
                first_frame=first_frame,
                last_frame=last_frame,
            )

    print("Done. Video đã cắt nằm trong:", segments_dir)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Cách chạy:")
        print('python cut_video_by_action_events.py "data/sessions/<ten_session>"')
        sys.exit(1)

    cut_session_video(sys.argv[1])