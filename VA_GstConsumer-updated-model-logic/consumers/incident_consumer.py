import argparse
import os
import time
import multiprocessing
from dotenv import load_dotenv
from pika.exceptions import AMQPConnectionError
from infer.incident_processor import RTSPFrameConsumer
from utils.utilities import arguments_parser, init_logger_alt

load_dotenv()
LOG_FOLDER = os.getenv('CONGESTION_LOG_FOLDER')
RABBIT_ID = os.getenv('RABBIT_ID')
RABBIT_PASS = os.getenv('RABBIT_PASS')
PRODUCER_IPV4 = os.getenv('PRODUCER_IPV4')
main_logger = init_logger_alt(f'{LOG_FOLDER}/main_log')


class ReconnectingConsumer(object):

    def __init__(self, amqp_url, camera, sl):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._location = camera
        self._main_logger = sl
        self._consumer = RTSPFrameConsumer(amqp_url=amqp_url, location=camera, main_logger=sl)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            except AMQPConnectionError as e:
                self._consumer.stop()
                time.sleep(self._get_reconnect_delay())
                self._remove_handlers()
                self._consumer = RTSPFrameConsumer(amqp_url=self._amqp_url, location=self._location,main_logger=self._main_logger)
            except Exception as e:
                self._consumer.stop()
                time.sleep(self._get_reconnect_delay())
                self._main_logger.error(f"{self._location}: Unknown error. Counts: {e.args}", e, exc_info=True)
                self._remove_handlers()
                break


    def _remove_handlers(self):
        if self._consumer.stream_logger.hasHandlers():
            for handler in self._consumer.stream_logger.handlers[:]:
                handler.close()
                self._consumer.stream_logger.removeHandler(handler)
                self._main_logger.info(f"{self._location}: Handler removed")
    
    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def run_consumer(amqp_url, camera, source_logger):
    consumer = ReconnectingConsumer(amqp_url=amqp_url, camera=camera, sl=source_logger)
    consumer.run()


def run_incident_consumers():
    parser = argparse.ArgumentParser(description="IncidentConsumer")
    parser.add_argument('--cameras_names', type=str, required=True, nargs='+', help='List of camera names')
    parser.add_argument('--services', choices=['IDL', 'CON', 'INC'], required=True, nargs='1',
                        help='List of services for each camera')
    args = parser.parse_args()
    incident_cameras = args.camera_names
    
    main_logger.info(f"Number of incident consumers: {len(args.camera_names)}")
    amqp_url = rf'amqp://{RABBIT_ID}:{RABBIT_PASS}@{PRODUCER_IPV4}:5672/%2F?connection_attempts=20&heartbeat=600'

    processes = []
    for cam in incident_cameras:
        process = multiprocessing.Process(target=run_consumer, args=(amqp_url, cam, main_logger))
        process.start()
        processes.append(process)

    main_logger.info(f"Started {len(processes)} incident consumers")

    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        main_logger.info("Interrupted by user. Stopping consumers...")
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        main_logger.info("All producers stopped")


if __name__ == '__main__':
    run_incident_consumers()
