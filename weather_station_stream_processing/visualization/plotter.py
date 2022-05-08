import random

import matplotlib.pyplot as plt

from weather_station_stream_processing import constants


class Plotter:
    def __init__(self, *args, plot_type: str = 'line', distinct_colors=False, unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND, **kwargs):
        """Class that is called with a datetime, value tuple and adds the value to a plot.

        :param args: positional arguments to the matplotlib axes plot function
        :param plot_type: type of plot to produce. Valid values are 'line' for a line plot and 'scatter' for a scatter plot
        :param distinct_colors: plot scatter plot points with distinct colors for each value or not
        :param kwargs: keyword arguments to the matplotlib axes plot function
        """

        if plot_type not in {'line', 'scatter', 'marked-line'}:
            raise ValueError('Value of argument plot_type should be \'line\', \'scatter\' or \'marked-line\'.')
        self._plot_type = plot_type
        self._fig, self._ax = plt.subplots()
        self._fig.autofmt_xdate()
        self._args = args
        self._kwargs = kwargs
        self._idx = 0
        self._unk_val = unk_val
        self.distinct_colors = distinct_colors
        self._colors = dict()

        self._prev_val = None

    def __call__(self, nxt_val):
        if self._plot_type == 'line' or self._plot_type == 'marked-line':
            outlier = False
            if self._plot_type == 'marked-line':
                nxt_val, outlier = nxt_val
            if self._prev_val:
                if nxt_val[1] == self._unk_val:
                    self._ax.plot((self._prev_val[0], nxt_val[0]), (self._prev_val[1], self._prev_val[1]), *self._args, color='blue', **self._kwargs)
                else:
                    self._ax.plot((self._prev_val[0], nxt_val[0]), (self._prev_val[1], nxt_val[1]), *self._args, color='blue', **self._kwargs)
                    self._prev_val = nxt_val
                    if outlier:
                        self._ax.plot((nxt_val[0]), (nxt_val[1]), 'o', color='green')
            else:
                self._prev_val = nxt_val
        elif self._plot_type == 'scatter':
            color = (0.0, 0.0, 1.0)
            if self.distinct_colors:
                color = self._colors.setdefault(nxt_val[1], [random.random() for _ in range(3)])
            self._ax.plot((nxt_val[0]), (nxt_val[1]), '.', *self._args, color=color, **self._kwargs)
