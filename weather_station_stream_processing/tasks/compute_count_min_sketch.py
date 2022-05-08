from typing import Tuple

from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.processing.count import compute_min_sketch_count, CountMinSketch


def get_stream_for_compute_count_min_sketch(stream: Stream, w: int, d: int) -> Tuple[Tuple[Stream, CountMinSketch], tuple]:
    """Get stream for computing counts of bucketed values using the Count-min sketch algorithm.

    :param stream: upstream
    :param w: the w parameter of the Count-min sketch algorithm (number of columns)
    :param d: the d parameter of the Count-min sketch algorithm (number of rows)
    :return: streamz stream for computing the counts using the min-sketch algorithm and the bucketing intervals
    """

    _LOW_BOUND = -10
    _HIGH_BOUND = 30
    _STEP = 5

    return compute_min_sketch_count(
        stream,
        col_name=constants.TEMP_COL_NAME,
        w=w,
        d=d,
        unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND,
        low_bound=_LOW_BOUND,
        high_bound=_HIGH_BOUND,
        step=_STEP
    ), tuple(range(_LOW_BOUND, _HIGH_BOUND + _STEP, _STEP))
