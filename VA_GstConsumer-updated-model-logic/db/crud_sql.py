import os
import uuid
from datetime import datetime, date
from sqlite3 import Date
from mysql.connector import connect
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("HOST")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")
class_names = {0: 'auto', 1: 'bus', 2: 'car', 3: 'motorbike', 4: 'truck', 5: 'person'}


def get_db_connection():
    connection = connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    return connection


def create_stream_quality_table(cursor) -> None:
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS stream_quality (
                event_id VARCHAR(255) PRIMARY KEY,
                location_id VARCHAR(100),
                date VARCHAR(100),
                up_time VARCHAR(100) NULL,
                down_time VARCHAR(100) NULL,
                response_code INT(10)
            )
        """)


def create_main_table(cursor) -> None:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_counts (
            location_id VARCHAR(255),
            Date DATE,
            Hour INT,
            Quarter INT,
            vehicle_id VARCHAR(255),
            count INT,
            source_id VARCHAR(255) PRIMARY KEY
        )
    """)


def update_main_db(connection, location_id: str, date: Date, hour: int,
                   quarter: int, vehicle_counts: dict[int, int]) -> None:
    cursor = connection.cursor()
    for i in range(6):
        source_id = f"{location_id}_{date}_{hour}_{quarter}_{class_names[i]}"
        cursor.execute("SELECT COUNT(*) FROM vehicle_counts WHERE source_id = %s", (source_id,))
        result = cursor.fetchone()
        if result[0] > 0:
            cursor.execute("UPDATE vehicle_counts SET count = %s WHERE source_id = %s", (vehicle_counts[i], source_id,))
        else:
            cursor.execute("""
                    INSERT INTO vehicle_counts (location_id, Date, Hour, Quarter, vehicle_id, count, source_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (location_id, date, hour, quarter, class_names[i], vehicle_counts[i], source_id))
        # cursor.close()
        connection.commit()


def update_stream_db(connection, location_id: str, date: date, status: bool,
                     event_time: datetime, error_code: int, logger) -> None:
    cursor = connection.cursor()
    try:
        if status:  # insert a new up_time event
            new_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO stream_quality (event_id, location_id, date, up_time, response_code) VALUES
                 (%s, %s, %s, %s, %s);
            """, (new_id, location_id, date, event_time, error_code, ))
            connection.commit()
            logger.info(f"Inserted new up_time: {event_time} for new ID : {new_id}")
        else:  # update an existing up_time event's down_time
            cursor.execute("""
                SELECT event_id FROM stream_quality
                WHERE location_id = %s
                ORDER BY up_time DESC
                LIMIT 1;
            """, (location_id, ))
            old_id = cursor.fetchone()[0]
            cursor.execute("""
                UPDATE stream_quality SET down_time = %s, response_code=%s WHERE event_id = %s;
            """, (event_time, error_code, old_id, ))
            connection.commit()
            logger.info(f"Updated down_time: {event_time} for old ID: {old_id}")
    except Exception as e:
        logger.error(f"DBWrite Exception occurred.", exc_info=True)
        raise e


def resume_count(connection, location_id: str, date: Date, hour: int, quarter: int, vehicle_counts: dict[int, int]) -> None:
    cursor = connection.cursor()
    for i in range(6):
        source_id = f"{location_id}_{date}_{hour}_{quarter}_{class_names[i]}"
        cursor.execute("SELECT count FROM vehicle_counts WHERE source_id = %s", (source_id,))
        result = cursor.fetchone()
        if result:
            vehicle_counts[i] = result[0] if result[0] > 0 else 0
        else:
            vehicle_counts[i] = 0
    update_main_db(connection, location_id, date, hour, quarter, vehicle_counts)
