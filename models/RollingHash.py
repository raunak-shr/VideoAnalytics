from typing import Generator, List
import os
import shutil
from ultralytics.engine.results import Results as Frame
import datetime
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

    def capture_frame(self) -> None:
        """
        Return a (new) directory path with the captured frames for saving the frames after processing the window

        Returns:
            Final results directory path
        """
        timestamp = str(datetime.datetime.now())
        self.ts = timestamp
        final_dir = os.path.join(self.result_dir, timestamp)
        if os.path.exists(final_dir):
            shutil.rmtree(final_dir)
        os.mkdir(final_dir)
        for idx, frame in enumerate(self.window):
            filename = f'{final_dir}/{timestamp} - Accident - {idx}.jpg'
            frame.plot(save=True, filename=filename)
        self.result_dir = final_dir

    def calculate_acc_percent(self):
        # Count occurrences of 1 within the window using the hash
        count_ones = sum(1 for bit in bin(self.hash_value)[2:] if bit == '1')
        return float(count_ones / self.window_size)

    def add_element(self, frame: Frame):
        if len(self.window) == self.window_size:
            self.hash_value -= self.window[0].probs.top1  # Update hash on element removal
            self.window.pop(0)

        self.window.append(frame)
        self.hash_value += 1  # Update hash with new element

        if len(self.window) == self.window_size:
            acc_frame_percent = self.calculate_acc_percent()
            if acc_frame_percent >= self.threshold:
                self.flag = True
                self.capture_frame()

    def is_flag_raised(self) -> bool:
        return self.flag

    def get_dir(self) -> str:
        return self.result_dir
