import numpy as np
from collections import defaultdict


class VehicleTracker:
    def __init__(self, vehicle_counts: dict[int, int], max_frames_absent: int = 10):
        """
        Each vehicle will store classes, frame_count, absent_frames,
        `class` will be a `defaultdict(int)` to store count of different classes the same tracking id has seen.
        Args:
            max_frames_absent (int): maximum number of frames to consider the vehicle as absent and evict it
        """

        self.vehicles = defaultdict(lambda: {
            "class": defaultdict(int),
            "frame_count": 0,
            "absent_frames": 0
        })
        self.max_frames_absent = max_frames_absent
        self.class_counts = vehicle_counts
        self.mapping = {0: 'auto', 1: 'bus', 2: 'car', 3: 'motorbike', 4: 'truck', 5: 'person'}

    def update(self, tracking_ids, classes):
        track_ids = np.array(tracking_ids)
        clss = np.array(classes)

        current_ids = set(track_ids)

        # update existing vehicles
        to_remove = []
        for tid, data in self.vehicles.items():
            if tid in current_ids:
                idx, = np.where(track_ids == tid)  # find index of the current id in the list of tracking ids
                current_class = clss[idx[0]] if len(idx) > 0 else None  # use the index to find its corresponding class
                if current_class is not None:
                    data["class"][current_class] += 1
                    data["frame_count"] += 1
                    data["absent_frames"] = 0

            else:
                data["absent_frames"] += 1
                if data["absent_frames"] > self.max_frames_absent:
                    most_seen_class = max(data["class"], key=data["class"].get)  # the class seen the most no. of times
                    self.class_counts[most_seen_class] += 1
                    to_remove.append(tid)

        # remove absent vehicles
        for tid in to_remove:
            del self.vehicles[tid]

        # add new vehicles
        new_ids = current_ids - self.vehicles.keys()
        if new_ids is not None:
            for tid in new_ids:
                idx, = np.where(track_ids == tid)
                current_class = clss[idx[0]]
                self.vehicles[tid] = {
                    "class": defaultdict(int, {current_class: 1}),
                    "frame_count": 1,
                    "absent_frames": 0
                }

    def get_class_counts(self, out_type: str = 'dct'):
        if out_type == 'dct':
            return self.class_counts
        elif out_type == 'lst':
            # print({self.mapping[k]: v for k, v in self.class_counts.items()})
            return [self.class_counts[key] if key in self.class_counts else 0 for key in range(6)]
