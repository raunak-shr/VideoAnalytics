import os
import cv2
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool

PATH = r"E:\VA\GstConsumer\frames"


def make_playbacks(directory):
    queue_name = directory.split('\\')[-1]
    files = os.listdir(directory)
    if len(files) < 1:
        return f"Empty Directory: {queue_name}"
    
    image_paths = [f'{directory}\\{x}' for x in files]
    image_paths.sort(key=lambda x: os.path.getctime(x))

    npy_files = image_paths

    os.makedirs(r'playbacks', exist_ok=True)
    first_frame = np.load(npy_files[0])

    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    out = cv2.VideoWriter(rf'playbacks/{queue_name}.avi', fourcc, 20.0, (first_frame.shape[1], first_frame.shape[0]))

    for idx, file in tqdm(enumerate(npy_files)):
        try:
            frame = np.load(file)
            frame = frame.astype(np.uint8)
            out.write(frame)
            os.remove(file)
        except Exception:
            print(f"Skipping frame {idx} for {queue_name}")
            os.remove(file)
            continue

    out.release()
    print(f"Video saved as {queue_name}.avi")


if __name__ == '__main__':
    directory_list = [fr'{PATH}\{x}' for x in os.listdir(f'{PATH}')]

    with Pool(processes=len(directory_list)) as pool:
        pool.map(make_playbacks, directory_list)
