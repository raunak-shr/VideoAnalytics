import os
import threading
import time

import gi
import logging
import pika
from utils.utilities import init_logger
from turbojpeg import TurboJPEG

from datetime import datetime, timedelta
import numpy as np

jpeg = TurboJPEG()
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
from gi.repository import Gst, GLib, GstApp

Gst.init(None)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Gst.init(None)
FAILS = 0
TRIALS = 0


class RTSPFrameProducer:
    def __init__(self, uri, queue_name, connection, main_logger, log_dir=r'logs/stream_logs'):

        self.uri = uri
        self.queue_name = queue_name
        self.pipeline = None
        self.appsink = None
        self.frame_count = 0
        self.msg_count = 0
        self.connection = connection
        self.channel = None
        self._start_time = datetime.now() + timedelta(milliseconds=0)
        self.fps = 18  # gives 14 fps while sending
        self.ml = main_logger
        self._unsent_frames = 0
        self._latest_msg_ts = None
        self._monitor_thread = None
        self._running = None
        self._retrying = None
        self._was_retrying = None

        os.makedirs(log_dir, exist_ok=True)
        self.stream_logger = init_logger(log_dir, self.queue_name, type='stream')

    def open_channel(self):
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
        )

    def setup_pipeline(self):
        try:
            pipeline_str = (
                f'rtspsrc location="{self.uri}" protocols=tcp latency=0 ! '
                f'rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! videorate ! '
                f'video/x-raw,format=BGR,framerate={self.fps}/1 ! queue ! appsink name=sink emit-signals=true'
            )

            self.pipeline = Gst.parse_launch(pipeline_str)
            self.appsink = self.pipeline.get_by_name(f'sink')
            self.appsink.connect('new-sample', self.on_new_sample)

            bus = self.pipeline.get_bus()
            bus.connect('message::error', self.on_error)
            bus.connect("message::eos", self.on_eos)
            bus.connect("message::warning", self.on_warning)
            bus.connect("message::state-changed", self.on_state_changed)
            if self._retrying is None:
                self.stream_logger.info(f"Pipeline set up successfully")
            else:
                self.stream_logger.info(f"Pipeline set up")
        except GLib.Error as e:
            logger.error(f"Error setting up pipeline: {e}")
            raise

    def start(self):
        self.open_channel()
        if not self._retrying:
            self.stream_logger.info(f"Starting pipeline for {self.uri}")
            self.pipeline.set_state(Gst.State.PLAYING)
            self._monitor_thread = threading.Thread(target=self.monitor_messages, args=())
            self._monitor_thread.start()
            self._running = True
        else:
            self.stream_logger.info(f"Trial: {TRIALS}. Retrying pipeline for: {self.uri}")
            self._start_time = datetime.now() + timedelta(milliseconds=0)
            self.pipeline.set_state(Gst.State.PLAYING)
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
            # time.sleep(50)
            self.connection.sleep(50)
        global FAILS, TRIALS
        while self._running:
            try:
                ts1 = datetime.strptime(self._latest_msg_ts, "%Y-%m-%d %H:%M:%S.%f")
                ts2 = datetime.now()
                if (ts2 - ts1).seconds > 120:
                    self._retrying = True
                    FAILS += 1
                    self.stream_logger.info(f"Pipeline stopped. Retrying pipeline in 15 seconds...")
                    self.ml.info(f"{self.queue_name}: Pipeline may be stopped. Retrying...")
                    while self._retrying:
                        TRIALS += 1
                        self.appsink = None
                        self.stop()
                        self._was_retrying = True
                        self.stream_logger.info(f"{self.queue_name}: Sleeping for restart...")
                        # time.sleep(15)  # #30 allow the ioloop to stop and thread to join
                        self.connection.sleep(15)
                        self.setup_pipeline()
                        self.start()
                        time.sleep(20)  # wait for 20 seconds to check again
                        # self.connection.sleep(20)
                        reference = datetime.now()
                        msg_ts = datetime.strptime(self._latest_msg_ts, "%Y-%m-%d %H:%M:%S.%f")
                        if (reference - msg_ts).seconds < 60:
                            self.stream_logger.info(f"Pipeline resumed. Latest message TS: {self._latest_msg_ts}.")
                            self._retrying = False
                        else:
                            self.stream_logger.info(f"Trial failed. Trying again in 15 seconds...")
                else:
                    # logger.info(f"Latest message timestamp: {self._latest_msg_ts}")
                    self.stream_logger.info(f"Latest message timestamp: {self._latest_msg_ts}")

                time.sleep(5)  # 10
            except Exception:
                self.stream_logger.info(f"Couldn't check time", exc_info=True)
                self._monitor_thread.join()
            except KeyboardInterrupt:
                self.stream_logger.info(f"{self.queue_name}: Interrupted. Stopping Monitoring")
                self._monitor_thread.join()

    def on_new_sample(self, _appsink):
        try:
            sample = self.appsink.pull_sample()
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

            self.frame_count += 1

            mem = memoryview(map_info.data)
            img = np.frombuffer(mem, dtype=np.uint8).reshape((height, width, 3))

            cmp_img = jpeg.encode(img, quality=20)

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
                                'resume': True if self._was_retrying else None},
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

            except Exception as e:
                self.ml.error(f"{self.queue_name}: Frame not sent: ", exc_info=True)

            # self.frame_logger.info(f"{self.frame_count} received from {self.queue_name}")
            # logger.info(f"{self.frame_count} received from {self.queue_name}")

            buffer.unmap(map_info)

            return Gst.FlowReturn.OK
        except Exception as e:
            self.ml.error(f"{self.queue_name}: Error in on_new_sample: {e}")
            return Gst.FlowReturn.ERROR

    def on_error(self, bus: Gst.Bus, message: Gst.Message):
        err, debug = message.parse_error()
        self.ml.info("Gstreamer.%s: Error %s: %s. ", self, err, debug)
        self.ml.debug(f"Debug info: {debug}")

    def on_eos(self, bus: Gst.Bus, message: Gst.Message):
        self.ml.info("Gstreamer.%s: Received stream EOS event. Calling stop...", self)
        self.stop()

    def on_warning(self, bus: Gst.Bus, message: Gst.Message):
        warn, debug = message.parse_warning()
        self.ml.warning("Gstreamer.%s: %s. %s", self, warn, debug)

    def on_state_changed(self, bus: Gst.Bus, message: Gst.Message):
        self.ml.info(f"State changed: {self.queue_name}")
        if message.src == self.pipeline:
            old_state, new_state, pending_state = message.parse_state_changed()
            self.ml.info(
                f"{self.queue_name}: Pipeline state changed from {old_state.value_nick} to {new_state.value_nick}")
