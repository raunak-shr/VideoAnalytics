import os
import uuid
import shutil
import datetime
from typing import List
from ultralytics.engine.results import Results as Frame
SAVE_LOCATION = r"test-outputs"  # save the uploaded video and outputs (if any) this location


class RollingHash:
    """
    Responsible for calculating the Rolling Hash for a window of a certain size. If accident is detected on a certain
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
        self.hash_value = 0
        self.ts = None
        folder = datetime.date.today().strftime("%d %B %Y")
        final_dir = os.path.join(self.result_dir, folder)
        if os.path.exists(final_dir):
            shutil.rmtree(final_dir)
        os.mkdir(final_dir)
        self.result_dir = final_dir

    def capture_frame(self) -> None:
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
            removed_frame = self.window.pop(0)
            if removed_frame.probs.top1 == 1:
                self.acc_frame_count -= 1

        self.window.append(frame)
        if frame.probs.top1 == 1:
            self.acc_frame_count += 1

        if len(self.window) == self.window_size:
            if self.acc_frame_count >= self.threshold * self.window_size:
                self.flag = True
            else:
                self.flag = False
        else:
            self.flag = False


    def is_flag_raised(self) -> bool:
        return self.flag

    def get_dir(self) -> str:
        return self.result_dir
