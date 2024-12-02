from collections import deque
from statistics import median, mean


class Rolling:
    def __init__(self, window=1):
        self.data = deque(maxlen=window)

    def append(self, datapoint):
        self.data.append(datapoint)

    def median(self):
        return median(self.data)

    def mean(self):
        return mean(self.data)
