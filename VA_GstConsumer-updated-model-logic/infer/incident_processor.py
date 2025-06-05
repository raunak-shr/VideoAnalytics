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
from pika.exceptions import AMQPConnectionError
from pika.adapters.asyncio_connection import AsyncioConnection
from infer.incident_result_processor import SequentialDeque


load_dotenv()
LOG_FOLDER = os.getenv('INCIDENT_LOG_FOLDER')
MODEL_PATH = os.getenv('INCIDENT_MODEL_PATH')

STREAM_STATUS = None
tracker_type: str = "bytetrack.yaml"
msg_count: int = 0
buffer_size: int = 25
jpeg = TurboJPEG()
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = YOLO(MODEL_PATH).to(device)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RTSPFrameConsumer:

    def __init__(self, amqp_url, location, main_logger,
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
        self.seque = None

        self.stream_start_time = None  # can be used while logging
        # self.db_connection = None  # pymongo connection

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
        self.seque: SequentialDeque = SequentialDeque(camera_name = self.queue_name)


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
        self.seque = None
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

    def on_message(self, _unused_channel, _basic_deliver, properties, body):
        global msg_count
        image = jpeg.decode(body)
        timestamp = datetime.strptime(properties.headers['timestamp'], "%Y-%m-%d %H:%M:%S.%f")
        result = model.predict(image)

        self.seque.process_result(result, timestamp, image)

        #-----------------------------------------------------------

        self._channel.basic_ack(_basic_deliver.delivery_tag)
        msg_count += 1
        logger.info(f"Processed {msg_count} | TS: {properties.headers['timestamp']} | "
                    f"Resume: {properties.headers['resume']}")

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
            raise AMQPConnectionError()
        except Exception as e:
            self.stream_logger.error("Unknown Error: ", e, exc_info=True)
            raise Exception()

    def stop(self):
        if not self._closing:
            self._closing = True
            self.stream_logger.info(f'Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.run_forever()
            else:
                self._connection.ioloop.stop()