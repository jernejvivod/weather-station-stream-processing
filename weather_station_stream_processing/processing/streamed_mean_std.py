import math

from weather_station_stream_processing import constants


class StreamedMeanStd:
    def __init__(self, unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND):
        """Compute streamed mean and standard deviation.

        :param unk_val: value signaling a missing/unknown value
        """

        self._unk_val = unk_val

        # current mean, variance and count of processed values
        self._mean = 0.0
        self._var = 0.0
        self._count = 0

    def __call__(self, x):
        prev_mean = self._mean
        prev_std = self._var

        # if not value signalling unknown/missing value, compute next mean and variance
        if x != self._unk_val:
            self._mean = ((prev_mean * self._count) + x) / (self._count + 1)
            self._var = ((self._var + prev_mean ** 2) * self._count + x ** 2) / (self._count + 1) - self._mean ** 2
            self._count += 1

        return {'mean': prev_mean, 'std': math.sqrt(prev_std if abs(prev_std) > 1.0e-16 else 0.0)}
