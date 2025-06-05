import os
import uuid
import base64
import requests
import datetime
import logging

from collections import deque
from dotenv import load_dotenv
from typing import Deque, Dict
from db.crud_mongo import push_to_incidents

load_dotenv()
disc_webhoook_url = os.getenv("DISCORD_WEBHOOK_URL")
save_location = os.getenv("INCIDENT_SAVE_LOCATION")

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SequentialDeque:
    def __init__(self, camera_name: str, window_size: int = 25,threshold: float = 0.75, order: int = 25) -> None:
        
        ''''''
        self.acc_pred_queue: Deque = deque(maxlen=window_size)  # The DeQue Window for observing the frames of accident (Consist of only flags)
        self.fire_pred_queue: Deque = deque(maxlen=window_size)  # The DeQue Window for observing the frames of fire (Consist of only flags)
        self.fight_pred_queue: Deque = deque(maxlen=window_size)  # The DeQue Window for observing the frames of fight (Consist of only flags)
        self.acc_frame_count = 0
        self.fire_frame_count = 0
        self.fight_frame_count = 0

        self.window_size: int = window_size  # Size of the window to account for

        self.ts: Deque[datetime.datetime] = deque(maxlen=30)  # Timestamp list of spikes
        self.threshold: float = threshold  # Threshold for classifying a frame as accident
        self.order: int = order  # No. of "continuous" spikes required for event notification/trigger
        self.incident_counts: Dict[str, int] = {"acc": 0, "fire": 0, "fight": 0}  # No. of incidents detected
        self.incident_ts: Dict[str, str] = {"acc": None, "fire": None, "fight": None}  # Timestamp of latest detected incident
        # self.json_value: Dict = {}  # Dictionary for logging
        self.currentDir: str = None  # Today's date directory
        self.camera_name: str = camera_name

        start_folder = datetime.date.today().strftime("%d %B %Y")
        init_day_dir = os.path.join(save_location, start_folder)
        os.makedirs(init_day_dir, exist_ok=True)
        self.currentDir = init_day_dir 


    def capture_frame(self, image, timestamp) -> str:
        """
        Captures the Current frame and saves to stream directory's current date folder.
        """
        self.ts.append(timestamp)
        time_stamp = self.ts[-1].strftime("%H:%M:%S").replace(":", "_")
        img_filename = os.path.join(self.currentDir, f'{time_stamp}_{uuid.uuid4()}')
        # img_data = base64.b64decode(b64_image)
        with open(img_filename, 'wb') as f:
            f.write(image)
            
        loc = f'{img_filename}.jpg'
        return loc


    def send_discord_alert(self, img_path: str, incident_type: str) -> requests.models.Response:
        """
        Send alert to discord channel with image, metadata.
        """
        files = {
            'payload_json': (
                None, 
                f'{{"content": "{incident_type} detected at {self.camera_name} at {self.incident_ts[incident_type]}"}}'
            ),
            'media': open(img_path, 'rb')
        }
        response = requests.post(disc_webhoook_url, files = files)
        return response
    

    @staticmethod
    def get_b64(img_path: str) -> str:
        ''' Returns the image encoded into base64 string '''
        with open(img_path, "rb") as img_file:
            encoded_string = base64.b64encode(img_file.read()).decode('utf-8')
        
        return encoded_string


    def notify_acc(self, image, timestamp) -> None:
        """
        Notify Accident as per logic in class definition. If the last 'order' no. of windows
        have a time difference of less than limit seconds then, notify.
        """
        logger.info("====================!! Accident Detected !!====================")
        img_path = self.capture_frame(image, timestamp)
        self.incident_ts["acc"] = str(self.ts[-1])
        r = self.send_discord_alert(img_path, "Accident")
        if r.status_code == 200:
            logger.info("Sent Accident alert to discord.")
        else:
            logger.info(f"!! Accident alert to discord failed. Response {r.status_code} !!")
        push_to_incidents(date=self.incident_ts["acc"], name="Accident", img_path=img_path)
        self.incident_counts["acc"] += 1


    def notify_fire(self,image, timestamp) -> None:
        """
        Notify Fire as per logic in class definition. If the last 'order' no. of windows
        have a time difference of less than limit seconds then, notify.
        """
        logger.info(f"====================!! Fire Detected !!====================")
        img_path = self.capture_frame(image, timestamp)
        self.incident_ts["fire"] = str(self.ts[-1])
        r = self.send_discord_alert(img_path, "Fire")    
        if r.status_code == 200:
            logger.info("Sent Fire alert to discord.")
        else:
            logger.info(f"!! Fire alert to discord failed. Response {r.status_code} !!")
        push_to_incidents(date=self.incident_ts["fire"], name="Fire", img_path=img_path)
        self.incident_counts["fire"] += 1

    def notify_fight(self,image, timestamp) -> None:
        """
        Notify Fight as per logic in class definition. If the last 'order' no. of windows
        have a time difference of less than limit seconds then, notify.
        """
        logger.info(f"====================!! Fire Detected !!====================")
        img_path = self.capture_frame(image, timestamp)
        self.incident_ts["fight"] = str(self.ts[-1])
        r = self.send_discord_alert(img_path, "Fight")    
        if r.status_code == 200:
            logger.info("Sent Fight alert to discord.")
        else:
            logger.info(f"!! Fight alert to discord failed. Response {r.status_code} !!")
        push_to_incidents(camera_name=self.camera_name, timestamp=self.incident_ts["fight"], name="Fight", img_path=img_path)
        self.incident_counts["fight"] += 1


    async def process_result(self, result, timestamp, image) -> None:
        '''
        Receives detection results from frames individually (multiple boxes per frame)
        '''

        acc_cls = 3
        fire_cls = 8
        fight_cls = 9

        frame_has_accident = False
        frame_has_fire = False
        frame_has_fight = False
        
        for box in result.boxes:
            class_id = int(box.cls)
            confidence = float(box.conf)
            
            if class_id == acc_cls and confidence > self.threshold:
                frame_has_accident = True
                
            if class_id == fire_cls and confidence > self.threshold:
                frame_has_fire = True

            if class_id == fight_cls and confidence > self.threshold:
                frame_has_fight = True
        
        if frame_has_accident:
            self.acc_pred_queue.append(1)
            self.acc_frame_count = min(25, self.acc_frame_count + 1)
        else:
            self.acc_pred_queue.append(0)
            self.acc_frame_count = max(0, self.acc_frame_count - 1)
            
        if frame_has_fire:
            self.fire_pred_queue.append(1)
            self.fire_frame_count = min(25, self.fire_frame_count + 1)
        else:
            self.fire_pred_queue.append(0)
            self.fire_frame_count = max(0, self.fire_frame_count - 1)

        if frame_has_fight:
            self.fight_pred_queue.append(1)
            self.fight_frame_count = min(25, self.fight_frame_count + 1)
        else:
            self.fight_pred_queue.append(0)
            self.fight_frame_count = max(0, self.fight_frame_count - 1)
                
        # For Accident Alert
        if self.acc_frame_count > int(0.88 * self.window_size):     # (22 out of 25 = 0.88)
            self.ts.append(timestamp)
            limit = 2  # In seconds: 2 sec = 50 frames
            if len(self.ts) >= self.order:
                time2 = self.ts[-1]
                time1 = self.ts[-self.order]
                time_diff = time2 - time1
                if time_diff.total_seconds() < limit:
                    await self.notify_acc(image,timestamp)
                    self.acc_frame_count = 0  # Clears the acc window
                    
        # For Fire Alert
        if self.fire_frame_count > int(0.88 * self.window_size):    # (22 out of 25 = 0.88)
            self.ts.append(timestamp)
            limit = 4  # In seconds: 4 sec = 100 frames
            if len(self.ts) >= self.order:
                time2 = self.ts[-1]
                time1 = self.ts[-self.order]
                time_diff = time2 - time1
                if time_diff.total_seconds() < limit:
                    await self.notify_fire(image, timestamp)
                    self.fire_frame_count = 0

        # For Fight Alert
        if self.fight_frame_count > int(0.88 * self.window_size):    # (22 out of 25 = 0.88)
            self.ts.append(timestamp)
            limit = 4  # In seconds: 4 sec = 100 frames
            if len(self.ts) >= self.order:
                time2 = self.ts[-1]
                time1 = self.ts[-self.order]
                time_diff = time2 - time1
                if time_diff.total_seconds() < limit:
                    await self.notify_fight(image, timestamp)
                    self.fight_frame_count = 0
                        