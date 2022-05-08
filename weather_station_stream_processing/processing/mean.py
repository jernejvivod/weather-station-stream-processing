from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.utils import annotation
from weather_station_stream_processing.utils.datetime import to_standard_format


def compute_mean_for_minutes(upstream: Stream,
                             col_name: str,
                             date_col_name: str,
                             time_col_name: str,
                             minutes: int = 60,
                             data_granularity_minutes: int = 5,
                             unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND
                             ) -> Stream:
    """Compute mean for minutes. The stream returns the starting datetime of the window and the mean as a tuple.

    :param upstream: upstream
    :param col_name: name of data column containing the value of interest
    :param date_col_name: name of data column containing the date
    :param time_col_name: name of data column containing the time
    :param minutes: size of averaging window in minutes
    :param data_granularity_minutes: data granularity in minutes
    :param unk_val: value signaling a missing/unknown value
    :return: the resulting stream
    """

    # compute mean of points
    def compute_mean(pts):
        filtered = tuple(filter(lambda x: x != unk_val, pts))
        return sum(filtered) / len(filtered) if len(filtered) > 0 else unk_val

    # partitioned and annotated stream
    stream_partitioned_annotated = upstream.map(annotation.annotate).partition(minutes // data_granularity_minutes)

    # stream of date and time for the partitions
    stream_datetime = stream_partitioned_annotated \
        .map(lambda x: to_standard_format(x[0][date_col_name], x[0][time_col_name]))

    # stream of means of partitions
    stream_mean = stream_partitioned_annotated \
        .map(lambda pts: tuple(map(lambda x: float(x[col_name]), pts))) \
        .map(compute_mean)

    # return zipped stream of dates for partitions and the stream of means of partitions
    return stream_datetime.zip_latest(stream_mean)
