import os
import gi
import pika
import time
import logging
import threading
import numpy as np
import pika.exceptions

from turbojpeg import TurboJPEG
from utils.utilities import init_logger
from datetime import datetime, timedelta
from pika.exceptions import ConnectionClosed
from pika.adapters.select_connection import SelectConnection

gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
from gi.repository import Gst, GLib, GstApp

jpeg = TurboJPEG()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
Gst.init(None)
FAILS = 0
TRIALS = 0


class RTSPFrameProducer:
    def __init__(self, uri, queue_name, rabbitmq_params, main_logger, streams_log_dir=r"logs/stream_logs",
                 frames_log_dir=r"logs/frame_logs"):
        self.uri = uri
        self.queue_name = queue_name
        self.pipeline = None
        self.appsink = None
        self.frame_count = 0
        self.msg_count = 0
        self.rabbitmq_params = rabbitmq_params
        self.connection = None
        self.channel = None
        self.bus = None
        self._start_time = datetime.now() + timedelta(milliseconds=0)
        self.fps = 18  # gives 14 fps while sending
        self.ml = main_logger
        self._iothread = None
        self._unsent_frames = 0
        self._latest_msg_ts = None
        self._prev_msg_ts = None
        self._monitor_thread = None
        self._thread_list = None
        self._running = None
        self._retrying = None
        self._was_retrying = None
        # self._deliveries = {}
        # self._acked = None
        # self._nacked = None
        # self._message_number = 0

        # Logging setup
        os.makedirs(streams_log_dir, exist_ok=True)
        self.stream_logger = init_logger(streams_log_dir, self.queue_name, type='stream')

        # os.makedirs(frames_log_dir, exist_ok=True)
        # self.frame_logger = init_logger(frames_log_dir, self.queue_name, type='frame')

        # os.makedirs(f'frames/{self.queue_name}', exist_ok=True)

    def setup_pipeline(self):
        try:
            # perfect
            pipeline_str = (
                f'rtspsrc location="{self.uri}" protocols=tcp latency=0 ! '
                f'rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! videorate ! '
                f'video/x-raw,format=BGR,framerate={self.fps}/1 ! queue ! appsink name=sink emit-signals=true'
            )
            self.pipeline = Gst.parse_launch(pipeline_str)
            appsink = self.pipeline.get_by_name('sink')
            appsink.connect('new-sample', self.on_new_sample)

            self.bus = self.pipeline.get_bus()
            self.bus.add_signal_watch()
            self.bus.connect('message', self.on_message)

            if self._retrying is None:
                self.stream_logger.info(f"Pipeline set up successfully")
            else:
                self.stream_logger.info(f"Pipeline set up")

        except GLib.Error as e:
            self.ml.error(f"{self.queue_name}: Error setting up pipeline: {e}")
            raise

    def start(self):
        if not self._retrying:
            logger.info(f"Starting pipeline for {self.uri}")
            self.pipeline.set_state(Gst.State.PLAYING)
            self.connect_to_rabbitmq()

            self._monitor_thread = threading.Thread(target=self.monitor_messages, args=())
            self._iothread = threading.Thread(target=self.connection.ioloop.start, args=())
            self._thread_list = [self._iothread, self._monitor_thread]
            for t in self._thread_list:
                t.start()
            self._running = True

        else:
            self.stream_logger.info(f"Trial: {TRIALS}. Retrying pipeline for: {self.uri}")
            self._start_time = datetime.now() + timedelta(milliseconds=0)
            self.pipeline.set_state(Gst.State.PLAYING)
            self.connect_to_rabbitmq()

            self._iothread = threading.Thread(target=self.connection.ioloop.start, args=())
            self._thread_list[0] = self._iothread
            self._iothread.start()
            self._running = True

    def stop(self):
        self.stream_logger.info(f"Setting pipeline state NULL.")
        self.pipeline.set_state(Gst.State.NULL)
        self.stream_logger.info(f"Pipeline set to NULL.")
        if not self._retrying:
            self._running = False
        if self.connection:
            self.connection.close()  # will join threads here

    def monitor_messages(self):
        time.sleep(30)
        if self._latest_msg_ts is None:  # stream was dead from start, stop both thread processes
            self.stop()
            time.sleep(50)
        global FAILS, TRIALS
        while self._running:
            try:
                if self._latest_msg_ts == self._prev_msg_ts:
                    self._retrying = True
                    FAILS += 1
                    self.stream_logger.info(f"Pipeline stopped. Retrying pipeline in 15 seconds...")
                    self.ml.info(f"{self.queue_name}: Pipeline may be stopped. Retrying...")
                    while self._retrying:
                        TRIALS += 1
                        self.stop()
                        self._was_retrying = True
                        self.stream_logger.info(f"{self.queue_name}: Sleeping for restart...")
                        time.sleep(10)  # #30 allow the ioloop to stop and thread to join
                        self.setup_pipeline()
                        self.start()
                        time.sleep(10)  # wait for 10 seconds to check again

                        if self._latest_msg_ts != self._prev_msg_ts:
                            self.stream_logger.info(f"Pipeline resumed. Latest message TS: {self._latest_msg_ts}.")
                            self._retrying = False
                        else:
                            self.stream_logger.info(f"Trial failed. Trying again in 15 seconds...")
                else:
                    # logger.info(f"Latest message timestamp: {self._latest_msg_ts}")
                    self._prev_msg_ts = self._latest_msg_ts
                    self.stream_logger.info(f"Latest message timestamp: {self._latest_msg_ts}")

                time.sleep(5)  # 10
            except Exception:
                self.stream_logger.info(f"Couldn't check time", exc_info=True)
                self._monitor_thread.join()
            except KeyboardInterrupt:
                self.stream_logger.info(f"{self.queue_name}: Interrupted. Stopping Monitoring")
                self._monitor_thread.join()

    def connect_to_rabbitmq(self):
        self.connection = SelectConnection(
            parameters=self.rabbitmq_params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )
        self.ml.info(f"{self.queue_name}: Connected to RabbitMQ")

    def on_connection_open(self, _connection):
        self.stream_logger.info("Connection opened")
        # self.connection.add_on_connection_blocked_callback(self.on_connection_blocked)
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_open_error(self, _unused_connection, err):
        self.ml.error(f"{self.queue_name}: Connection open failed: {err}")

    def on_connection_closed(self, _unused_connection, reason):
        self.stream_logger.info(f"Connection closed: {reason}")
        self.connection.ioloop.call_later(5, self.connection.ioloop.stop)
        # self.connection = None
        self.stream_logger.info(f"IOLoop Stopped.")

    def on_channel_open(self, channel):
        logger.info("Channel opened")
        self.channel = channel
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            callback=self.on_queue_declareok
        )

    def on_queue_declareok(self, _unused_frame):
        self.stream_logger.info(f"Stream queue {self.queue_name} declared")

    def on_new_sample(self, _appsink):
        try:
            sample = _appsink.pull_sample()
            if not sample:
                self.ml.warning(f"{self.queue_name}: Received empty sample")
                return Gst.FlowReturn.OK

            buffer = sample.get_buffer()
            ts = self._start_time + timedelta(milliseconds=(buffer.pts / Gst.MSECOND))
            success, map_info = buffer.map(Gst.MapFlags.READ)
            if not success:
                self.ml.error(f"{self.queue_name}: Failed to map buffer")
                return Gst.FlowReturn.ERROR

            caps = sample.get_caps()
            structure = caps.get_structure(0)

            success, width = structure.get_int("width")
            success, height = structure.get_int("height")

            mem = memoryview(map_info.data)
            img = np.frombuffer(mem, dtype=np.uint8).reshape((height, width, 3))
            cmp_img = jpeg.encode(img, quality=20)
            self.frame_count += 1
            try:
                if self.channel and self.channel.is_open:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.queue_name,
                        body=cmp_img,
                        properties=pika.BasicProperties(
                            content_type='application/octet-stream',
                            delivery_mode=2,
                            headers={
                                'timestamp': str(ts),
                                'resume': True if self._was_retrying else None
                            },
                        )
                    )
                    self.msg_count += 1
                    self._latest_msg_ts = str(ts)
                    # self.frame_logger.info(
                    #     f"Received: {self.frame_count} | Sent: {self.msg_count} | "
                    #     f"TS: {self._latest_msg_ts} | Resume: {self._was_retrying}"
                    # )
                    self._was_retrying = None

                    logger.info(
                        f"Received: {self.frame_count} | Sent: {self.msg_count} | "
                        f"TS: {self._latest_msg_ts} | Resume: {self._was_retrying}"
                    )
                else:
                    self._unsent_frames += 1
                    self.stream_logger.warning(f"Channel not available, frame not sent: {self._unsent_frames}")

            except Exception:
                self.ml.error(f"{self.queue_name}: Frame not sent: ", exc_info=True)
            except ConnectionClosed:
                self.stream_logger.error(f"Connection was closed abruptly.", exc_info=True)
                self.stop()
            except pika.exceptions.StreamLostError:
                self.stream_logger.error(f"Stream connection lost.", exc_info=True)
                self.stop()

            # self.frame_logger.info(f"{self.frame_count} received from {self.queue_name}")
            # logger.info(f"{self.frame_count} received from {self.queue_name}")

            buffer.unmap(map_info)

            return Gst.FlowReturn.OK
        except Exception as e:
            self.ml.error(f"{self.queue_name}: Error in on_new_sample: {e}")
            return Gst.FlowReturn.ERROR

    def on_message(self, _bus, message):
        msg_type = message.type

        if msg_type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            self.ml.info("Gstreamer.%s: Error: %s ", self, err)
            if debug:
                self.stream_logger.info(f"Debug info: {debug}")

        elif msg_type == Gst.MessageType.EOS:
            self.stream_logger.info("End of Stream (EOS) reached.")

        elif msg_type == Gst.MessageType.WARNING:
            warn, debug = message.parse_warning()
            self.stream_logger.info(f"Warning: {warn.message}")
            if debug:
                self.stream_logger.info(f"Debug info: {debug}")

        elif msg_type == Gst.MessageType.INFO:
            info, debug = message.parse_info()
            self.stream_logger.info(f"Info: {info.message}")
            if debug:
                self.stream_logger.info(f"Debug info: {debug}")

        elif msg_type == Gst.MessageType.STATE_CHANGED:
            old_state, new_state, pending_state = message.parse_state_changed()
            self.stream_logger.info(
                f"State changed from {Gst.Element.state_get_name(old_state)} to "
                f"{Gst.Element.state_get_name(new_state)}")

        elif msg_type == Gst.MessageType.BUFFERING:
            percent = message.parse_buffering()
            self.stream_logger.info(f"Buffering... {percent}%")

        elif msg_type == Gst.MessageType.TAG:
            tags = message.parse_tag()
            self.stream_logger.info(f"Tags: {tags.to_string()}")

        elif msg_type == Gst.MessageType.DURATION_CHANGED:
            self.stream_logger.info("Duration changed.")

        elif msg_type == Gst.MessageType.LATENCY:
            self.stream_logger.info("Latency changed.")

        else:
            self.stream_logger.info(f"Unhandled message type: {msg_type}")
