import os
import shutil
import json
import uuid
import datetime
from typing import Deque, Dict, List
from PIL import Image
from collections import deque
from ultralytics import YOLO
from ultralytics.engine.results import Results as Frame
SAVE_LOCATION = r"test-outputs"  # save the uploaded video and outputs (if any) to this location
MODEL_X = YOLO(r'YOLOmodel/bestX.pt')


class SequentialDeque:
    """
    Checks for a sequence of windows to be classified as an accident and notifies to client. If 'x' 'continuous' 
    windows are judged as accidents then only notification is sent (sequence detected as accident). A window is 
    classified as accident if ((window_size - 2) out of (window_size)) frames are classified as accident. A frame is 
    classified as accident if the prediction probability of accident by the model is greater than the given threshold
    """
    
    def __init__(self, window_size: int = 30, threshold: float = 0.8, order: int = 3):
        self.prediction_queue: Deque = deque(maxlen = window_size)
        self.window_size: int = window_size
        self.result_dir: str = SAVE_LOCATION
        self.fse: int = 0  # frames_since_event
        self.ts: List[str] = []  # Timestamp list of spikes
        self.threshold: float = threshold  # Threshold above which a frame will be termed as accident
        self.order: int = order # No. of "continuous" spikes required for event notification  ## define continuity...... (within 2 secs, 3 secs, 4 secs, ???)
        self.spikes: int = 0  # No. of spikes detected
        self.acc_count: int = 0  # No. of accidents detected
        self.acc_ts = None  # Timestamp of detected accident
        self.json_value: Dict = {}  # Dictionary for logging
        
        folder = datetime.date.today().strftime("%d %B %Y")
        final_dir = os.path.join(self.result_dir, folder)
        if os.path.exists(final_dir):
            shutil.rmtree(final_dir)
        # os.mkdir(final_dir)
        # self.result_dir = final_dir

    def capture_frame(self) -> None:
        """
        Captures the window frames to SAVE_LOCATION and append spike timstamps to the result json
        """
        folder = datetime.date.today().strftime("%d %B %Y")  # Create a folder with name of today's date
        final_dir = os.path.join(self.result_dir, folder)
    
        if not os.path.exists(final_dir):
            # shutil.rmtree(final_dir)
            os.mkdir(final_dir)
        
        self.result_dir = final_dir
        self.ts.append(datetime.datetime.now())  # Set time at which sequence is captured
        self.acc_ts = self.ts[-1]  # can make it a list append to store multiple accidnets in a day
        for _, frame in enumerate(self.prediction_queue):
            frame.plot(save=True, filename=f'{self.result_dir}/{uuid.uuid4()}.jpg')
    
    def dump_json(self, spike_count: int) -> None:
        """
        Dump jsons containing event information to SAVE_LOCATION

        Args: spike_count: No. of spikes that took place
        """
        
        fold = datetime.date.today().strftime("%d %B %Y")  # Create a folder with name as event's date
        final_dir = os.path.join(self.result_dir, fold)
        if not os.path.exists(final_dir):
            os.mkdir(final_dir)
        
        json_path = f'{SAVE_LOCATION}\{fold}\Accident.json'
        self.json_value[f"Spike {spike_count}"] = {
            "Timestamp": str(self.ts[-1]), 
            "Incident": "Accident", 
            "Probabilities": str([list(x.probs.data.numpy()) for x in self.prediction_queue]),
            "Accident Count": self.acc_count
        }

        with open(json_path, "w") as f:
            json.dump(self.json_value, f)
            f.write('\n')
    
    def final_test(self):
        image_list = [Image.fromarray(x.orig_img) for x in self.prediction_queue]
        conf_list = []
        for img in image_list:
            res = MODEL_X.predict(img, save = False)
            try:
                conf_list.append(res.boxes.conf.numpy()[0])  # if there's a bounding box detection, append its confidence
            except:
                conf_list.append(0.0)  # else append 0.0
        if (len(conf_list) - conf_list.count(0))>self.window_size-2:  # if >23/25 frames have bounding boxes
            self.capture_frame()
            self.acc_count+=1
            print("\n====================!! Accident Detected !!====================")
    
    def notify(self):
        """
        Notify to the client as per logic in class definition. If the last 'order' no. of windows have a time difference of 
        less than 1 seconds (25 frames (25fps stream)) then, notify.
        """
        if len(self.ts)>=self.order:
            time2 = self.ts[-1]             # Say order = 4, ts_array = [ts1, ts2, ts3, ..., ts9, ts10, ts11, ts12]. 
            time1 = self.ts[-self.order]    # In case accident happend ts9 to ts 12 must've been under a 
            time_diff = time2 - time1       # short span of (self.order/25) seconds therefore ts12 - ts9 < (self.order/25)
            if time_diff.total_seconds() < int(self.order/25):
                # self.capture_frame()
                self.final_test()
                # self.acc_count+=1
                # print("\n====================!! Accident Detected !!====================")
                
    
    def add_element(self, frame: Frame):
        self.prediction_queue.append(frame)
        if len(self.prediction_queue) == self.window_size:
           
            events = [1 if x.probs.data[1]>self.threshold else 0 for x in self.prediction_queue]
            acc_frame_count = events.count(1)
            if acc_frame_count>(self.window_size-2) and self.fse == 0:  # self.window_size - 2 = border (24,25)
                print(f"\nSpike detected! Prediction: {acc_frame_count/len(events):.2f}\n")
                self.ts.append(datetime.datetime.now())
                self.spikes+=1
                self.notify()
                self.dump_json(self.spikes)
                self.fse = 1
            else:
                print(f"\tPrediction: {acc_frame_count/len(events):.2f}")
            if self.fse > 0:
                self.fse += 1
            if self.fse == 10:
                self.fse = 0
    
                

        


