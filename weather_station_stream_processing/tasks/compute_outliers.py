from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.processing.outliers import compute_outliers


def get_stream_for_compute_outliers(stream: Stream) -> Stream:
    """Get stream for marking outliers (for task 3).

    :param stream: source stream
    :return: streamz stream for computing the outliers.
    """

    return compute_outliers(
        stream,
        col_name=constants.TEMP_COL_NAME,
        date_col_name=constants.DATE_COL_NAME,
        time_col_name=constants.TIME_COL_NAME,
        unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND,
        std_outlier_criteria=3.0,
    )
