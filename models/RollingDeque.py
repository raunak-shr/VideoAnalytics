import os
import shutil
import json
import uuid
import datetime
from typing import Deque
from collections import deque
from ultralytics.engine.results import Results as Frame
SAVE_LOCATION = r"test-outputs"  # save the uploaded video and outputs (if any) to this location


class RollingDeque:
    def __init__(self, window_size: int, threshold: float):
        self.prediction_queue: Deque = deque(maxlen = window_size)
        self.threshold = threshold
        self.window_size = window_size
        self.result_dir = SAVE_LOCATION
        self.fse = 0  # frames_since_event
        self.ts = None  # Timestamp of detected incident
        self.triggers = 0  # No. of event triggers
        folder = datetime.date.today().strftime("%d %B %Y")
        final_dir = os.path.join(self.result_dir, folder)
        if os.path.exists(final_dir):
            shutil.rmtree(final_dir)
        os.mkdir(final_dir)
        self.result_dir = final_dir

    def capture_frame(self) -> None:
        """
        Captures the window to SAVE_LOCATION
        """
        self.ts = str(datetime.datetime.now())  # Set time at which sequence is captured
        
        for _, frame in enumerate(self.prediction_queue):
            frame.plot(save=True, filename=f'{self.result_dir}/{uuid.uuid4()}.jpg')
    
    def dump_json(self, no_of_events: int) -> None:
        """
        Dump jsons containing event information to SAVE_LOCATION
        """
        
        fold = datetime.date.today().strftime("%d %B %Y")
                
        json_path = f'{SAVE_LOCATION}\{fold}\Accident.json'
        json_value = {"Timestamp": self.ts, 
                        "Incident": "Accident",
                        "No. of triggers": no_of_events, 
                        "Probabilities": str([list(x.probs.data.numpy()) for x in self.prediction_queue])}

        with open(json_path, "w") as f:
            json.dump(json_value, f)
    
    def add_element(self, frame: Frame):
        self.prediction_queue.append(frame)
        if len(self.prediction_queue) == self.window_size:
           
            events = [1 if x.probs.data[1]>self.threshold else 0 for x in self.prediction_queue]
            acc_frame_count = events.count(1)
            if acc_frame_count>(self.window_size-2) and self.fse == 0:
                print(f"\nEvent triggered! Prediction: {acc_frame_count/len(events):.2f}")
                self.triggers+=1
                self.capture_frame()
                self.dump_json(self.triggers)
                self.fse = 1
            else:
                print(f"\nPrediction: {acc_frame_count/len(events):.2f}")
            if self.fse > 0:
                self.fse += 1
            if self.fse == 10:
                self.fse = 0
                

        


