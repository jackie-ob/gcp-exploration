from contextlib import contextmanager
import time


@contextmanager
def measure(label, work_qty=None, work_unit=None):
    t0 = time.time()
    splits = []

    def add_split(split_label):
        splits.append((split_label, time.time()))

    try:
        yield add_split
    finally:
        duration = time.time() - t0
        print("%s %.3f" % (label, duration))
        if work_qty is not None:
            assert work_unit is not None
            print("Rate: %.2f %s per second" % (work_qty/duration, work_unit))
        split_durations = [None] * len(splits)
        for i in range(len(splits)):
            if i == 0:
                split_durations[i] = splits[i][1] - t0
            else:
                split_durations[i] = splits[i][1] - splits[i - 1][1]
        for i, sd in enumerate(split_durations):
            print("    %20s: %-.2f" % (splits[i][0], split_durations[i]))
