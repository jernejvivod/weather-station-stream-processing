from typing import Tuple

import streamz
from streamz import Stream

from weather_station_stream_processing.utils import annotation
from weather_station_stream_processing.utils.datetime import to_standard_format


def compute_index_tuple_max_for_minutes(upstreams: Tuple[Stream],
                                        col_name: str,
                                        date_col_name: str,
                                        time_col_name: str,
                                        minutes: int = 60,
                                        data_granularity_minutes: int = 5
                                        ) -> Stream:
    """Compute mean for minutes. The stream returns the starting datetime of the window and the mean as a tuple.

    :param upstreams: upstreams
    :param col_name: name of data column containing the value of interest
    :param date_col_name: name of data column containing the date
    :param time_col_name: name of data column containing the time
    :param minutes: size of averaging window in minutes
    :param data_granularity_minutes: data granularity in minutes
    :return: the resulting stream
    """

    # partitioned and annotated stream
    streams_partitioned = [stream.map(annotation.annotate).partition(minutes // data_granularity_minutes) for stream in upstreams]

    # stream of date and time for the partitions
    stream_dates = streams_partitioned[0] \
        .map(lambda pts: to_standard_format(pts[0][date_col_name], pts[0][time_col_name]))

    # streams of maximum values
    streams_max = [stream.map(lambda pts: tuple(map(lambda x: float(x[col_name]), pts))).map(max) for stream in streams_partitioned]

    # stream of indices of the stream with the maximal value
    stream_index_max = streamz.zip_latest(*streams_max).map(lambda x: x.index(max(x)) + 1)

    # returned zipped stream of dates and the stream of indices of points with maximal specified value
    return stream_dates.zip_latest(stream_index_max)
