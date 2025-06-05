import os
from datetime import datetime


def calculate_average_frame_rate(log_file):
    with open(log_file, 'r') as file:
        lines = file.readlines()
    
    if len(lines) < 2:
        return 0  
    
    timestamps = []
    
    for line in lines:
        timestamp_str = line.split(' - ')[0]
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
        timestamps.append(timestamp)
    
    time_differences = [
        (timestamps[i] - timestamps[i-1]).total_seconds() for i in range(1, len(timestamps))
    ]
    
    if time_differences:
        avg_frame_rate = 1 / (sum(time_differences) / len(time_differences))
    else:
        avg_frame_rate = 0
    
    return avg_frame_rate


def main(logs_folder):
    print("Starting")
    for log_file in os.listdir(logs_folder):
        log_file_path = os.path.join(logs_folder, log_file)
        if os.path.isfile(log_file_path) and log_file.endswith(".log"):
            location_name = os.path.splitext(log_file)[0]
            avg_frame_rate = calculate_average_frame_rate(log_file_path)
            print(f"Location: {location_name}, Average Frame Rate: {avg_frame_rate:.2f} FPS")


if __name__ == "__main__":
    logs_folder = os.path.join("logs")
    main(logs_folder)
