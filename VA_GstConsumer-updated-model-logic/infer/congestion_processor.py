import os
import pika
import torch
import logging
import functools
from ultralytics import YOLO
from dotenv import load_dotenv
from turbojpeg import TurboJPEG
from utils.utilities import init_logger
from datetime import datetime, timedelta
from infer.vehicle_tracker import VehicleTracker
from pika.exceptions import AMQPConnectionError
from pika.adapters.asyncio_connection import AsyncioConnection
from db.crud_sql import update_main_db, resume_count, get_db_connection

load_dotenv()
LOG_FOLDER = os.getenv('CONGESTION_LOG_FOLDER')
MODEL_PATH = os.getenv('CONGESTION_MODEL_PATH')
STREAM_STATUS = None
tracker_type: str = "bytetrack.yaml"
confidence: float = 0.3
iou: float = 0.8
buffer_size: int = 25
date_format = "%Y-%m-%d %H:%M:%S.%f"
jpeg = TurboJPEG()
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = YOLO(MODEL_PATH).to(device)
vc_dict = None
msg_count = 0
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RTSPFrameConsumer:

    def __init__(self, amqp_url, location, vehicle_counts, main_logger,
                 streams_log_dir=f'{LOG_FOLDER}/streams', frames_log_dir=f'{LOG_FOLDER}/frames'):

        self.should_reconnect = False
        self.was_consuming = False
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        self._prefetch_count = 0
        self.queue_name = location
        self.ml = main_logger

        self.current_date = None
        self.stream_start_time = None
        self.previous_ts = None
        self.previous_quarter = None
        self.db_connection = None

        self.tracker = None
        self.vehicle_count = vehicle_counts

        # logging Setup
        self.stream_logger = init_logger(streams_log_dir, self.queue_name, type='stream')
        # self.frame_logger = init_logger(frames_log_dir, self.queue_name, type='frames')
        # os.makedirs(f'frames/{self.queue_name}', exist_ok=True)

    def connect(self):
        self.stream_logger.info('Connecting to %s', self._url)
        return AsyncioConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self.ml.info(f'{self.queue_name}: Connection is closing or already closed')
        else:
            self.ml.info(f'{self.queue_name}: Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        self.stream_logger.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        self.ml.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.ml.warning(f'{self.queue_name}: Connection closed, reconnect necessary: %s.', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        self.stream_logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self.stream_logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()

        self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def add_on_channel_close_callback(self):
        self.stream_logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        self.ml.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def on_basic_qos_ok(self, _unused_frame):
        self.stream_logger.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        self.stream_logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.queue_name, self.on_message, auto_ack=False)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        self.stream_logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        self.ml.info(f'{self.queue_name}: Consumer was cancelled remotely, shutting down: %r',
                     method_frame)
        if self._channel:
            self._channel.close()

    def update_tracker(self, results):
        if hasattr(results, 'boxes') and results.boxes is not None:
            tracking_ids = getattr(results.boxes, 'id', None)
            classes = getattr(results.boxes, 'cls', None)
            if tracking_ids is not None and classes is not None:
                self.tracker.update(tracking_ids, classes)
        return self.tracker.get_class_counts(out_type='dct')

    def setup_attributes(self, properties):
        if properties.headers['resume'] is not None:  # implement stream quality check code
            self.stream_logger.info(f"Stream was offline, fetching latest counts.")
        else:
            self.stream_logger.info(f"Starting processing with frame TS: {properties.headers['timestamp']}")
        self.stream_start_time = datetime.strptime(properties.headers['timestamp'], date_format)
        self.current_date = self.stream_start_time.date()
        self.previous_ts = self.stream_start_time
        self.previous_quarter: int = int((self.previous_ts.minute / 15) + 1)
        self.db_connection = get_db_connection()
        self.vehicle_count: dict[int, int] = {key: 0 for key in range(7)}
        resume_count(self.db_connection, self.queue_name, self.stream_start_time.date(),
                     self.stream_start_time.hour, int((self.stream_start_time.minute / 15) + 1), self.vehicle_count)
        self.stream_logger.info(f"{self.queue_name}: Fetched latest count: {self.vehicle_count}")
        self.tracker = VehicleTracker(max_frames_absent=10, vehicle_counts=self.vehicle_count)

    def change_quarter(self, curr_ts, curr_quarter):
        update_main_db(self.db_connection, self.queue_name, self.previous_ts.date(), self.previous_ts.hour,
                       self.previous_quarter, self.vehicle_count)  # update counts from first frame of next quarter
        counts = {self.tracker.mapping[k]: v for k, v in self.tracker.class_counts.items()
                  if self.tracker.mapping[k] not in ['rider', 'person']}
        self.stream_logger.info(f"Finished Hour: {self.previous_ts.hour} | Quarter: {self.previous_quarter} | "
                                f"Counts: {counts}")
        self.previous_ts = curr_ts
        self.previous_quarter = curr_quarter
        self.vehicle_count = {key: 0 for key in range(7)}
        self.tracker = VehicleTracker(max_frames_absent=10, vehicle_counts=self.vehicle_count)

    def on_message(self, _unused_channel, _basic_deliver, properties, body):

        global vc_dict, msg_count
        msg_count += 1
        if msg_count == 1 or properties.headers['resume'] is not None:
            self.setup_attributes(properties)

        frame = jpeg.decode(body)
        current_ts = datetime.strptime(properties.headers['timestamp'], date_format)

        current_quarter: int = int((current_ts.minute / 15) + 1)
        results = model.track(frame, persist=True, conf=0.3, iou=0.8)[0]
        self.vehicle_count = self.update_tracker(results)

        if current_quarter != self.previous_quarter:
            self.change_quarter(current_ts, current_quarter)

        update_main_db(self.db_connection, self.queue_name, current_ts.date(), current_ts.hour,
                       current_quarter, self.vehicle_count)
        vc_dict = self.vehicle_count

        if timedelta(days=1) <= (current_ts.date() - self.current_date):
            self.current_date = current_ts.date()
            self.stream_logger.info("End of the day")
            if self.stream_logger.hasHandlers():
                for handler in self.stream_logger.handlers[:]:
                    handler.close()
                    self.stream_logger.removeHandler(handler)
            self.stream_logger = init_logger(r"logs/stream_logs", self.queue_name, 'stream')
            self.stream_logger.info("Started inferencing for next day")

        self._channel.basic_ack(_basic_deliver.delivery_tag)

        # self.frame_logger.info(f"Processed {msg_count} | TS: {properties.headers['timestamp']} | "
        #                        f"Resume: {properties.headers['resume']} | Counts: {self.vehicle_count}"
        #                        )

        logger.info(f"Processed {msg_count} | TS: {properties.headers['timestamp']} | "
                    f"Resume: {properties.headers['resume']} | Counts: {self.vehicle_count}"
                    )

    def stop_consuming(self):
        if self._channel:
            self.stream_logger.info(f'Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        self.stream_logger.info(
            f'RabbitMQ acknowledged the cancellation of the consumer: %s', userdata)
        self.close_channel()

    def close_channel(self):
        self.stream_logger.info(f'Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        try:
            self._connection.ioloop.run_forever()
        except AMQPConnectionError as e:
            self.stream_logger.error("AMQP Connection Error: ", e, exc_info=True)
            raise AMQPConnectionError(vc_dict)
        except Exception as e:
            self.stream_logger.error("Unknown Error: ", e, exc_info=True)
            raise Exception(vc_dict)

    def stop(self):
        if not self._closing:
            self._closing = True
            self.stream_logger.info(f'Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.run_forever()
            else:
                self._connection.ioloop.stop()
