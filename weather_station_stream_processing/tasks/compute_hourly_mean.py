from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.processing.mean import compute_mean_for_minutes


def get_stream_for_compute_hourly_mean_temperature(stream: Stream) -> Stream:
    """Get stream for computing hourly mean temperature (for task 1).

    :param stream: source stream
    :return: streamz stream for computing the hourly mean temperature.
    """

    return compute_mean_for_minutes(
        stream,
        col_name=constants.TEMP_COL_NAME,
        date_col_name=constants.DATE_COL_NAME,
        time_col_name=constants.TIME_COL_NAME,
        minutes=60,
        data_granularity_minutes=constants.DATA_GRANULARITY_MIN,
        unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND
    )
