# Change directory to this folder and run using python -m uvicorn main_seque:app --reload --port 8001
import torch
from fastapi import FastAPI
from typing import Generator
from models.model import IpModel
from ultralytics import YOLO
from models.model import IpModel
from fastapi.exceptions import HTTPException
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from models.SequentialDeque import SequentialDeque
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
def upload_video_and_process(data: IpModel) -> None:
    """
    Upload a video stream (RTSP Link) and detect accidents

    Args:
        data: The RTSP Link(data.stream_link) and the threshold to classify a frame as accident
    Returns:
        dict: JSON format containing the Status ('1', '0'), the frames save path and the timestamp
    """
    try:
        source = data.file
        seque = SequentialDeque(window_size = 25, threshold = data.threshold, order = 25)  # check for atleast 2 seconds / 25 frames 
        results: Generator = MODEL.predict(source, stream=True, save=False)
        while True:
            f: Frame = next(results)
            seque.add_element(f)

    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to process video", headers={"X-Error": f"{e}"})
