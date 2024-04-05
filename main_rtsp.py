# Change directory to this folder and run using python -m uvicorn main_rtsp:app --reload --port 8001
import os
import torch
import shutil
import datetime
import supervision as sv
from fastapi import FastAPI
from typing import Generator
from ultralytics import YOLO
from fastapi.exceptions import HTTPException
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from models.model import IpModel


MODEL = YOLO(r'YOLOmodel/best3.pt')
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
SAVE_LOCATION = r"test-outputs"  # save the uploaded video and outputs (if any) this location
API_KEY = 'NgZkaQV5UzqGh8exm4d6'  # FROM ROBOFLOW

# FastAPI Setup---------------------------------------------------------------------------------------------------------

app = FastAPI(
    title="Video Analytics",
    description="""Detect for accidents in uploaded video (bytes format)
                    and return json results""",
    version="2023.1.31",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def redirect():
    return RedirectResponse("/docs")


# Support Functions-----------------------------------------------------------------------------------------------------
def save_file() -> str:
    """
    Return a (new) directory path for saving the frames after processing

    Returns:
        Final results directory path
    """
    final_dir = os.path.join(SAVE_LOCATION, 'Detections')
    if os.path.exists(final_dir):
        shutil.rmtree(final_dir)
    os.mkdir(final_dir)
    return final_dir


# MAIN function---------------------------------------------------------------------------------------------------------
@app.post("/event_detection_to_json")
def upload_video_and_process(data: IpModel) -> dict:
    """
    Upload a video stream (RTSP Link) and detect accidents

    Args:
        data: The RTSP Link(data.stream_link)
    Returns:
        dict: JSON format containing the Status ('1', '0') and the frames save path
    """
    try:
        # # Single stream with batch-size 1 inference
        # source0 = 'rtsp://example.com/media.mp4'  # RTSP, RTMP, TCP or IP streaming address
        # # Multiple streams with batched inference (i.e. batch-size 8 for 8 streams)
        # source = 'path/to/list.streams'  # *.streams text file with one streaming address per row
        # # video_info = sv.VideoInfo.from_video_path(source)
        source = data.file
        frames_per_sec = sv.VideoInfo.from_video_path(source).fps
        # noinspection PyTypeChecker
        results: Generator = MODEL.predict(source, stream=True, save=False)
        while True:
            result = next(results)
            if result.probs.top1 == 1:
                window = []
                json_list = []
                for _ in range(20):   # window length of 20
                    next_frame = next(results)
                    window.append(next_frame)
                events = [x.probs.top1 for x in window]
                acc_frame_percent = float(events.count(1) / len(events))
                if acc_frame_percent >= data.threshold:
                    result_dir = save_file()  # make directory for saving accident frames
                    for idx, frame in enumerate(window):  # Accident frames in the window is above threshold

                        timestamp = datetime.datetime.now()  # 0. Timestamp of frame

                        filename = f'{result_dir}/{timestamp} - Accident-{idx}.jpg'

                        frame.plot(save=True, filename=filename)  # 1. Save the frames

                        json_list.append({'Timestamp': timestamp, 'Incident': 'Accident'})  # 2. Create JSON List

                    return {'Status': 1, 'OutputPath': result_dir, 'JSONList': json_list}  # 3. Return Results

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to process video", headers={"X-Error": f"{e}"})
