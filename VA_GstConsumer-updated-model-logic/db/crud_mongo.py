import os
import logging
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
db_name = os.getenv("DB_NAME")
client = MongoClient('localhost', 27017)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
        

def push_to_incidents(camera_name: str, timestamp: datetime, name: str, b64_image: str) -> None:
    """
    Push the incident info to MongoDB Instance

    Args:
        date: Timestamp of the incident
        name: Name of the incident
        b64_img: Base64 encoded image to string
    """
    stream = client[db_name]["Streams"].find_one({"CameraName":camera_name})
    id = stream["_id"]

    document = {
        "IncTimestamp": str(timestamp),
        "IncName": name,
        "IncImage": [b64_image],
        "IncRTSP": id,
    }
    collection = client[db_name]["Incidents"]
    result = collection.insert_one(document)
    IncId = repr(result.inserted_id)
    logger.info(f"Inserted incident info with {IncId} into Collection: '{collection}' .")
