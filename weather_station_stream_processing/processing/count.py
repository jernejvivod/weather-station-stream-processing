import math
import os
from typing import Tuple

import mmh3
import numpy as np
from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.utils import annotation


def bucket_value(x, low_bound, high_bound, step):
    """Compute index of the bin of a given value given a low bound, a high bound and a step size.

    The values with value lower than the low bound are assigned to the first bin and values higher than
    the high bound are assigned to the last bin.

    :param x: value to bucket
    :param low_bound: low bound
    :param high_bound: high bound
    :param step: step size
    :return: index of the bin to which the value is assigned
    """
    if x < low_bound:
        return 0
    elif x >= high_bound:
        return ((high_bound - low_bound) // step) + 1
    else:
        add_val = 0
        if low_bound != 0:
            add_val = -low_bound
        return int((x + add_val) // step) + 1


class CountMinSketch:
    def __init__(self,
                 w: int,
                 d: int,
                 bucket: bool = False,
                 low_bound: float = -10.0,
                 high_bound: float = 30,
                 step: float = 5.0,
                 unk_val=None
                 ):
        """Count-min sketch algorithm implementation.

        :param w: the w parameter (number of columns)
        :param d: the d parameter (number of rows/hash functions)
        :param bucket: bucket the values or not
        :param low_bound: low bound for the bucketing operation
        :param high_bound: high bound for the bucketing operation
        :param step: step for the bucketing operation
        :param unk_val: value signaling a missing/unknown value
        """

        self.w = w
        self.d = d
        self.bucket = bucket

        self.low_bound = low_bound
        self.high_bound = high_bound
        self.step = step

        self.unk_val = unk_val

        if self.bucket:
            if (self.high_bound - self.low_bound) % self.step != 0:
                raise ValueError('The length of the bucketing interval should be divisible by the step size.')

            # number of bytes needed to represent the index of and assigned bucket (used for hashing)
            self._n_bytes_for_bucket_index = int(math.log(((self.high_bound - self.low_bound) / self.step) + 2, 256)) + 1

        hash_func_seeds = [int.from_bytes(os.urandom(16), 'big') for _ in range(self.d)]
        self._hash_funcs = [lambda x, s=seed: mmh3.hash(x, s) for seed in hash_func_seeds]
        self._cms_mat = np.zeros((self.d, self.w), dtype=int)

    def _ind_cols_cms_mat(self, x):
        """Get indices of columns in the Count-min sketch matrix for a given value.

        :param x: value for which to compute the column indices
        :return: the computed column indices
        """
        return [self._hash_funcs[idx](x) % self.w for idx in range(len(self._hash_funcs))]

    def _increment(self, x):
        """Increment applicable values in the Count-min sketch matrix for a given value.

        :param x: value for which to increment the Count-min sketch matrix
        """
        ind_cols = self._ind_cols_cms_mat(x)
        self._cms_mat[np.arange(self._cms_mat.shape[0]), ind_cols] += 1

    def query(self, x):
        """Query the Count-min sketch implementation for an approximation of a count of a given value.

        :param x: value for which to retrieve an approximation of the count
        :return: approximated count for the provided value
        """

        if self.bucket:
            bucketed_index = bucket_value(x, self.low_bound, self.high_bound, self.step)
            query_val = int.to_bytes(bucketed_index, self._n_bytes_for_bucket_index, 'big')
            ind_cols = self._ind_cols_cms_mat(query_val)
        else:
            ind_cols = self._ind_cols_cms_mat(x)
        return np.min(self._cms_mat[np.arange(self._cms_mat.shape[0]), ind_cols])

    def __call__(self, x):
        """Pass next data point to the Count-min sketch implementation.

        :param x: data point passed to the Count-min sketch implementation
        """
        if self.unk_val and x == self.unk_val:
            return
        if self.bucket:
            bucketed_index = bucket_value(x, self.low_bound, self.high_bound, self.step)
            self._increment(int.to_bytes(bucketed_index, self._n_bytes_for_bucket_index, 'big'))
        else:
            self._increment(x)


class CountExact:
    def __init__(self,
                 bucket: bool = False,
                 low_bound: float = -10.0,
                 high_bound: float = 30,
                 step: float = 5.0,
                 unk_val=None
                 ):
        """Implementation of exact counting of values.

        :param bucket: bucket the values or not
        :param low_bound: low bound for the bucketing operation
        :param high_bound: high bound for the bucketing operation
        :param step: step for the bucketing operation
        :param unk_val: value signaling a missing/unknown value
        """

        self.bucket = bucket

        self.low_bound = low_bound
        self.high_bound = high_bound
        self.step = step

        self.unk_val = unk_val

        if self.bucket and (self.high_bound - self.low_bound) % self.step != 0:
            raise ValueError('The length of the bucketing interval should be divisible by the step size.')

        self._counts = dict()

    def query(self, x):
        """Query the exact counting implementation for the count of a given value.

        :param x: value for which to retrieve the count
        :return: approximated count for the provided value
        """

        if self.bucket:
            bucketed_index = bucket_value(x, self.low_bound, self.high_bound, self.step)
            return self._counts[bucketed_index] if bucketed_index in self._counts else 0
        else:
            return self._counts[x] if x in self._counts else 0

    def __call__(self, x):
        """Pass next data point to the exact counting implementation.

        :param x: data point passed to the exact counting implementation
        """
        if self.unk_val and x == self.unk_val:
            return
        if self.bucket:
            bucketed_index = bucket_value(x, self.low_bound, self.high_bound, self.step)
            if bucketed_index not in self._counts:
                self._counts[bucketed_index] = 0
            self._counts[bucketed_index] += 1
        else:
            if x not in self._counts:
                self._counts[x] = 0
            self._counts[x] += 1


def compute_min_sketch_count(upstream: Stream,
                             col_name: str,
                             w,
                             d,
                             unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND,
                             low_bound: float = -10,
                             high_bound: float = 30,
                             step: float = 5
                             ) -> Tuple[Stream, CountMinSketch]:
    """Compute element counts using the Count-min sketch algorithm. The stream increments the counts of bucketed values using the Count-min sketch algorithm.
    The instance encapsulating the Count-min sketch count algorithm can be queried for the bucketed values to obtain the approximate counts.

    :param upstream: upstream
    :param col_name: name of data column containing the value of interest
    :param w: the w parameter of the Count-min sketch algorithm (number of columns)
    :param d: the d parameter of the Count-min sketch algorithm (number of rows)
    :param unk_val: value signaling a missing/unknown value
    :param low_bound: lower bound for the bucketing interval
    :param high_bound: upper bound for the bucketing interval
    :param step: bucketing step size
    :return: the resulting stream and the Count-min sketch implementation instance
    """

    cms = CountMinSketch(w, d, bucket=True, low_bound=low_bound, high_bound=high_bound, step=step, unk_val=unk_val)

    # annotated stream of specified data
    stream_data_annotated = upstream.map(annotation.annotate).map(lambda x: float(x[col_name]))

    # stream for counting bucketed values using the Count-min sketch algorithm
    stream_count_min_sketch_bucketed = stream_data_annotated.sink(cms)

    # return the stream for computing the count and the Count-min sketch implementation instance
    return stream_count_min_sketch_bucketed, cms


def compute_exact_count(upstream: Stream,
                        col_name: str,
                        unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND,
                        low_bound: float = -10,
                        high_bound: float = 30,
                        step: float = 5
                        ) -> Tuple[Stream, CountExact]:
    """Compute element counts using exact counting. The stream increments the counts of bucketed values using exact counting.
    The instance encapsulating exact counting can be queried for the bucketed values to obtain the approximate counts.

    :param upstream: upstream
    :param col_name: name of data column containing the value of interest
    :param unk_val: value signaling a missing/unknown value
    :param low_bound: lower bound for the bucketing interval
    :param high_bound: upper bound for the bucketing interval
    :param step: bucketing step size
    :return: the resulting stream
    """

    ce = CountExact(bucket=True, low_bound=low_bound, high_bound=high_bound, step=step, unk_val=unk_val)

    # annotated stream of specified data
    stream_data_annotated = upstream.map(annotation.annotate).map(lambda x: float(x[col_name]))

    # stream for counting bucketed values using exact counting
    stream_exact_count_bucketed = stream_data_annotated.sink(ce)

    # return the stream for computing the count and the exact counting implementation instance
    return stream_exact_count_bucketed, ce
