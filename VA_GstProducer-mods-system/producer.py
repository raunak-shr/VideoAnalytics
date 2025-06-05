import os
import gi
import pika
import pandas as pd
import multiprocessing
from dotenv import load_dotenv
from utils.utilities import init_logger_alt
from stream_handler.async_producer import RTSPFrameProducer

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib


load_dotenv()

GST_DEBUG = os.getenv('GST_DEBUG')
GST_DEBUG_FILE = os.getenv('GST_DEBUG_FILE')

Gst.init(None)
main_logger = init_logger_alt('logs/main_log')


def start_producer(uri, loc, rabbitmq_params, ml):
    producer = RTSPFrameProducer(
        uri=uri,
        queue_name=loc,
        rabbitmq_params=rabbitmq_params,
        main_logger=ml
    )
    producer.setup_pipeline()
    producer.start()


def main():
    rabbitmq_params = pika.ConnectionParameters(
        host='localhost',
        port=5672,
        virtual_host='/',
        credentials=pika.PlainCredentials('guest', 'guest'),
        heartbeat=600,
        blocked_connection_timeout=900,
        connection_attempts=20,
        channel_max=60,
    )

    df = pd.read_excel("RTSPs_traffic.xlsx")
    main_logger.info(f"Number of RTSP streams: {len(df)}")

    unstable = (0, 7, 8, 9, 10, 11, 12, 14, 15, 19, 20, 21, 22, 23, 24)
    indices = [x for x in range(0, len(df)-1, 1) if x not in unstable][:15]
    df_working = df.iloc[indices]

    uris = df_working.RTSPLink
    locations = df_working.Location

    main_logger.info(f"Starting processes for: {locations.to_list()}")

    processes = []
    for uri, loc in zip(uris, locations):
        process = multiprocessing.Process(target=start_producer, args=(uri, loc, rabbitmq_params, main_logger))
        process.start()
        processes.append(process)

    main_logger.info(f"Started {len(processes)} RTSP stream producers")

    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        main_logger.info("Interrupted by user. Stopping producers...")
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        main_logger.info("All producers stopped")


if __name__ == '__main__':
    main()
