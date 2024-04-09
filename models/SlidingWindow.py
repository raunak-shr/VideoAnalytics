import os
import shutil
import datetime
import uuid
from typing import List
from ultralytics.engine.results import Results as Frame

SAVE_LOCATION = r"test-outputs"  # save the uploaded video and outputs (if any) this location


class SlidingWindow:
    """
    Responsible for calculating the SlidingWindow for a window of a certain size. If accident is detected on a certain
    frame, -> saves/captures the frames to a directory (controlled by SAVE_LOCATION) if a certain condition is met.

    Attributes:
        window_size (int): Size of the window to consider for the rolling hash
        window (List): List containing the frames to consider for the rolling hash
        threshold (int): Threshold to consider to "classify a window" as accident sequence
        flag (bool): # Flag variable to check if accident is detected or not after adding a frame to the window
        result_dir (str): Path to the directory to save the results to (controlled by SAVE_LOCATION)
        ts (timestamp): Timestamp of the captured incident
    """

    def __init__(self, window_size: int, threshold: float):
        self.window_size = window_size
        self.window: List[Frame] = []
        self.threshold = threshold
        self.flag = False
        self.result_dir = SAVE_LOCATION
        self.ts = None
        folder = datetime.date.today().strftime("%d %B %Y")
        final_dir = os.path.join(self.result_dir, folder)
        if os.path.exists(final_dir):
            shutil.rmtree(final_dir)
        os.mkdir(final_dir)
        self.result_dir = final_dir
    
    def capture_window(self) -> None:
        """
        Return a (new) directory path with the captured frames for saving the frames after processing the window

        Returns:
            Final results directory path
        """
        self.ts = str(datetime.datetime.now())  # Set time at which sequence is captured
        
        for _, frame in enumerate(self.window):
            # time = str(datetime.datetime.now().strftime("%H:%M:%S"))
            frame.plot(save=True, filename=f'{self.result_dir}/{uuid.uuid4()}.jpg')
        
    def add_element(self, frame: Frame):
        if len(self.window) == self.window_size:
            self.window.pop(0)

        self.window.append(frame)

        if len(self.window) == self.window_size:
            for idx, frame in enumerate(self.window):
                if frame.probs.top1 == 1:
                    events = [x.probs.top1 for x in self.window]
                    acc_frame_percent = float(events.count(1) / len(events))
                    if acc_frame_percent >= self.threshold:
                        self.flag = True
                        break
            if self.flag == True:
                self.capture_window()
                self.flag = False
            else:
                self.flag = False
        

    def is_flag_raised(self) -> bool:
        return self.flag

    def get_dir(self) -> str:
        return self.result_dir
