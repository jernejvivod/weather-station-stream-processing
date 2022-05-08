from weather_station_stream_processing import constants


def annotate(pt: str):
    """Annotate data point (columns separated by whitespace).

    :param pt: input data
    :return: dictionary mapping column names to values
    """
    return {k: v for k, v in zip(constants.DATA_POINT_COLS_CHARS, pt.split())}
