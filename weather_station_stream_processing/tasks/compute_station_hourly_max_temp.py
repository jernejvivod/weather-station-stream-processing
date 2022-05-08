from typing import Tuple

from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.processing.index_max import compute_index_tuple_max_for_minutes


def get_stream_for_compute_station_with_hourly_max_temperature(streams: Tuple[Stream]) -> Stream:
    """Get stream for computing index of station with highest hourly temperature (for task 2).

    :param streams: tuple of source streams
    :return: streamz stream for computing the index of the station with the highest hourly temperature.
    """

    return compute_index_tuple_max_for_minutes(
        streams,
        col_name=constants.TEMP_COL_NAME,
        date_col_name=constants.DATE_COL_NAME,
        time_col_name=constants.TIME_COL_NAME,
        minutes=60,
        data_granularity_minutes=constants.DATA_GRANULARITY_MIN
    )
