import datetime


def to_standard_format(date, time):
    """map concatenated character representations of date and time to a datetime.datetime instance.

    :param date: concatenated character representation of date
    :param time: concatenated character representation of time
    :return: datetime.datetime instance representing the input date
    """

    return datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:]), int(time[:2]), int(time[2:]))
