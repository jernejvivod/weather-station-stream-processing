from typing import Tuple

from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.processing.count import compute_exact_count, CountExact


def get_stream_for_compute_count_exact(stream: Stream) -> Tuple[Tuple[Stream, CountExact], tuple]:
    """Get stream for computing counts of bucketed values using the Count-min sketch algorithm.

    :param stream: upstream
    :return: streamz stream for computing the counts using exact counting and the bucketing intervals
    """

    _LOW_BOUND = -10
    _HIGH_BOUND = 30
    _STEP = 5

    return compute_exact_count(
        stream,
        col_name=constants.TEMP_COL_NAME,
        unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND,
        low_bound=-10,
        high_bound=30,
        step=5
    ), tuple(range(_LOW_BOUND, _HIGH_BOUND + _STEP, _STEP))
