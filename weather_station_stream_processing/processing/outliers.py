from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.processing.streamed_mean_std import StreamedMeanStd
from weather_station_stream_processing.utils import annotation
from weather_station_stream_processing.utils.datetime import to_standard_format


def compute_outliers(upstream: Stream,
                     col_name: str,
                     date_col_name: str,
                     time_col_name: str,
                     unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND,
                     std_outlier_criteria: float = 3.0
                     ) -> Stream:
    """Compute outliers in the stream by examining how many standard deviations from the mean of the
    data observed so far a new value lies.

    :param upstream: upstream
    :param col_name: name of data column containing the value of interest
    :param date_col_name: name of data column containing the date
    :param time_col_name: name of data column containing the time
    :param unk_val: value signaling a missing/unknown value
    :param std_outlier_criteria: how many standard deviations away from the mean
    should a value be considered an outlier
    :return: the resulting stream
    """

    # annotated stream
    stream_annotated = upstream.map(annotation.annotate)

    # data stream
    stream_data = stream_annotated.map(lambda x: float(x[col_name]))

    # stream of date and time for data points
    stream_dates = stream_annotated \
        .map(lambda x: to_standard_format(x[date_col_name], x[time_col_name]))

    # stream of date and time for data points with the specified data value and a flag indicating if a
    # value is an outlier or not.
    stream_outliers = stream_annotated \
        .map(lambda pt: float(pt[col_name])) \
        .map(StreamedMeanStd(unk_val=unk_val)) \
        .zip_latest(stream_dates, stream_data) \
        .map(lambda x: ((x[1], x[2]), x[2] > x[0]['mean'] + std_outlier_criteria * x[0]['std'] or x[2] < x[0]['mean'] - std_outlier_criteria * x[0]['std']))

    return stream_outliers
