import os
import torch
import shutil
import base64
import supervision as sv
from fastapi import FastAPI
from ultralytics import YOLO
from fastapi.exceptions import HTTPException
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from api.model import IpModel

MODEL = YOLO(r'YOLOmodel/best3.pt')
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
SAVE_LOCATION = r"test-outputs"  # save the uploaded video and outputs (if any) this location
MAX_VIDEO_LENGTH_SEC = 240  # not more than 4 minutes

# FastAPI Setup--------------------------------------------------------------------------------------------------

app = FastAPI(
    title="Video Analytics",
    description="""Detect for accidents in uploaded video (bytes format)
                    and return json results via saving the accident frames""",
    version="2023.1.31",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def redirect():
    return RedirectResponse("/docs")


# Support Functions----------------------------------------------------------------------------------------
def save_file(video_bytes: bytes) -> dict:
    """
    Save the uploaded file somewhere for processing.

    Args:
        video_bytes: Uploaded video file in bytes format
    Returns:
        None
    """
    final_path = os.path.join(SAVE_LOCATION, 'Detections', 'uploaded.mp4')
    final_dir = os.path.join(SAVE_LOCATION, 'Detections')
    if os.path.exists(final_dir):
        shutil.rmtree(final_dir)
    os.mkdir(final_dir)
    with open(final_path, "wb") as f:
        f.write(video_bytes)
    result = {'Final Dir': final_dir, "Video Path": final_path}
    return result


def calculate_end_frame_index(source_video_path: str) -> int:
    """
    Calculate the no. of frames to process.  Currently limits to 4 mins

    Args:
        source_video_path: Path of the uploaded video
    Returns:
        int: No. of frames to process, the last frame if video length <= 4 mins
    """
    video_info = sv.VideoInfo.from_video_path(source_video_path)
    return min(
        video_info.total_frames,
        video_info.fps * MAX_VIDEO_LENGTH_SEC
    )


# MAIN function------------------------------------------------------------------------------------------

@app.post("/event_detection_to_json")
def upload_video_and_process(data: IpModel) -> dict:
    """
    Upload a video in bytes format and detect accidents

    Args:
        data: The video file (data.file) and the threshold (data.threshold)
    Returns:
        dict: JSON format containing the Status ('1', '0') and the frames save path
    """
    try:
        video = base64.b64decode(data.file)
        save_paths = save_file(video)
        source = save_paths['Video Path']
        video_info = sv.VideoInfo.from_video_path(source)

        results = MODEL.predict(source, stream=True, save=False, show_conf=False,
                                imgsz=(video_info.height, video_info.width))

        inference_frames = []
        for frame in results:
            inference_frames.append(frame)
        frames_per_sec = sv.VideoInfo.from_video_path(source).fps
        result_dir = save_paths['Final Dir']

        for frame_idx in range(len(inference_frames)):
            window = inference_frames[frame_idx:frame_idx + 20]  # current window size of 20
            events = [x.probs.top1 for x in window]
            json_list = []
            acc_frame_percent = float(events.count(1) / len(events))
            if acc_frame_percent >= data.threshold:  # save the window if count of accidents (1)
                for idx, frame in enumerate(window):  # in the window is above threshold

                    timestamp = round(float(idx / frames_per_sec), 3)  # 0. Calculate Timestamp of frame

                    filename = f'{result_dir}/{timestamp} - Accident-{idx}.jpg'

                    frame.plot(save=True, filename=filename)  # 1. Save the frames

                    json_list.append({'Timestamp': timestamp, 'Incident': 'Accident'})  # 2. Create JSON List

                return {'Status': 1, 'OutputPath': result_dir, 'JSONList': json_list}  # 3. Return Results
            else:
                return {'Status': -1, 'OutputPath': None, 'JSONList': json_list}

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to process video", headers={"X-Error": f"{e}"})
