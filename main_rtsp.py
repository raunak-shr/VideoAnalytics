# Change directory to this folder and run using python -m uvicorn main_rtsp_alt:app --reload --port 8001
import json
import torch
import datetime
import supervision as sv
from fastapi import FastAPI
from typing import Generator
from models.model import IpModel
from ultralytics import YOLO
from models.model import IpModel
from fastapi.exceptions import HTTPException
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from models.SlidingWindow import SlidingWindow as SlidWin
from ultralytics.engine.results import Results as Frame

MODEL = YOLO(r'YOLOmodel/best3.pt')
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
SAVE_LOCATION = r"test-outputs"

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

# MAIN function---------------------------------------------------------------------------------------------------------
@app.post("/detect")
def upload_video_and_process(data: IpModel) -> dict:
    """
    Upload a video stream (RTSP Link) and detect accidents

    Args:
        data: The RTSP Link(data.stream_link)
    Returns:
        dict: JSON format containing the Status ('1', '0'), the frames save path and the timestamp
    """
    try:
        source = data.file
        frames_per_sec = sv.VideoInfo.from_video_path(source).fps
        results: Generator = MODEL.predict(source, stream=True, save=False)
        sliding_window = SlidWin(window_size=20, threshold=data.threshold)
        json_value = None
        while True:
            f: Frame = next(results)
            sliding_window.add_element(f)
            if sliding_window.is_flag_raised():
                
                fold = datetime.date.today().strftime("%d %B %Y")
                
                json_path = f'{SAVE_LOCATION}\{fold}\Accident.json'
                json_value = {"Timestamp": sliding_window.ts, 
                              "Incident": "Accident", 
                              "Probabilities": str([list(x.probs.data.numpy()) for x in sliding_window.window])}
    
                with open(json_path, "w") as f:
                    json.dump(json_value, f)
                sliding_window.flag = False
                    # return {'Status': sliding_window.flag,
                    #         'OutputPath': sliding_window.result_dir,
                    #         'Timestamp': sliding_window.ts}  # 3. Return Results

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to process video", headers={"X-Error": f"{e}"})
