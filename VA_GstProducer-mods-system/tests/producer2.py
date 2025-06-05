import multiprocessing

import gi
import os
import pika
import logging
import traceback
import pandas as pd
from dotenv import load_dotenv
from utils.utilities import init_logger_alt
from datetime import time
from pika.adapters.blocking_connection import BlockingConnection
from stream_handler.blocking_producer import RTSPFrameProducer

gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
from gi.repository import Gst, GLib, GObject

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Gst.init(None)
cpu_count = os.cpu_count()
readers = []  

load_dotenv()

GST_DEBUG = os.getenv('GST_DEBUG')
GST_DEBUG_FILE = os.getenv('GST_DEBUG_FILE')
NUMEXPR_MAX_THREADS = os.getenv("NUMEXPR_MAX_THREADS")

logger.info(f"NUMEXPR_MAX_THREADS set to: {NUMEXPR_MAX_THREADS}")

main_logger = init_logger_alt(r'logs/main_log')


def start_producer(uri, queue_name, connection_params, source_logger):
    connection = BlockingConnection(connection_params) 
    reader = RTSPFrameProducer(uri, queue_name, connection, source_logger)
    reader.setup_pipeline()
    reader.start()
    source_logger.info(f"Started frame production for {queue_name}")


def main():

    df = pd.read_excel("RTSPs_traffic.xlsx")
    main_logger.info(f"Number of RTSP streams: {len(df)}")

    unstable = (0, 7, 8, 9, 10, 11, 12, 14, 15, 19, 20, 21, 22, 23, 24)
    indices = [x for x in range(0, len(df) - 1, 1) if x not in unstable][:15]
    df_working = df.iloc[indices]

    uris = df_working.RTSPLink
    locations = df_working.Location
       
    rabbitmq_params = pika.ConnectionParameters(
        host='localhost',
        port=5672,
        virtual_host='/',
        credentials=pika.PlainCredentials('guest', 'guest'),
        heartbeat=60,
        connection_attempts=20,
        channel_max=60,
    )
    
    main_logger.info("Starting GLib MainLoop")
    processes = []
    # for uri, loc in zip(uris, locations):
    #     process = multiprocessing.Process(target=start_producer, args=(uri, loc, rabbitmq_params, main_logger))
    #     process.start()
    #     processes.append(process)
    for uri, queue_name in zip(uris, locations):
        connection = BlockingConnection(rabbitmq_params)
        reader = RTSPFrameProducer(uri, queue_name, connection, main_logger)
        reader.setup_pipeline()
        reader.start()

    main_logger.info(f"Started {len(uris)} RTSP stream producers")
    loop = GLib.MainLoop()

    try:
        loop.run()
        main_logger.info(f"Started Glib Main Loop.")
        # for process in processes:
        #     process.join()
    except KeyboardInterrupt:
        logger.info("Interrupted. Stopping readers...")
        main_logger.info("Stopping readers.")
    except Exception as e:
        logger.error("Exception: ", e)
        main_logger.debug(traceback.format_exc(), exc_info=True)
    finally:
        for reader in readers:
            reader.stop()
        logger.info("All readers stopped")


if __name__ == '__main__':
    main()