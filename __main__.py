import argparse
import os
import pathlib

import matplotlib.pyplot as plt
from streamz import Stream

from weather_station_stream_processing import constants
from weather_station_stream_processing.tasks import compute_hourly_mean, \
    compute_station_hourly_max_temp, \
    compute_outliers, \
    compute_count_min_sketch, \
    compute_count_exact
from weather_station_stream_processing.visualization.plotter import Plotter


def main(task: int, dataset_path: str, plot_path: str, no_title: bool, w: int, d: int):
    """Perform computations and get plots for the tasks described in the README

    :param task: task index
    :param dataset_path: path to dataset(s) to use
    :param plot_path: path to folder in which to save plots
    :param no_title: omit title from plots or not
    :param w: the w parameter for the Min-count sketch algorithm
    :param d: the d parameter for the Min-count sketch algorithm
    """

    # initialize stream source
    src = Stream()

    """Task 1 - Compute the hourly temperature (hourly mean) for each station."""
    if task == 1:
        if len(dataset_path) > 1:
            raise ValueError('Only a single dataset should be specified for task 1.')

        stream = compute_hourly_mean.get_stream_for_compute_hourly_mean_temperature(src)
        stream.sink(Plotter(plot_type='line', unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND, linewidth=0.7))

        # stream data from file
        with open(dataset_path[0], 'r') as f:
            for line in f:
                src.emit(line)

        file_name_stem = pathlib.Path(dataset_path[0]).stem
        plt.ylabel('Temperature in degrees Celsius')
        if not no_title:
            plt.title('Hourly Mean Temperatures for {0}'.format(file_name_stem))
        plt.savefig(os.path.join(plot_path, '{0}_hourly_mean.png'.format(file_name_stem)))
        src.visualize(os.path.join(plot_path, 'stream_task_1.png'))

    """Task 2 - Stream temperature data from the three stations and report the station with the highest hourly temperature (use the subhourly data)."""
    if task == 2:
        if len(dataset_path) <= 1:
            raise ValueError('More than one dataset should be specified for task 2.')

        # sources
        srcs = tuple(Stream() for _ in range(len(dataset_path)))

        stream = compute_station_hourly_max_temp.get_stream_for_compute_station_with_hourly_max_temperature(srcs)
        stream.sink(Plotter(plot_type='scatter', distinct_colors=True, unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND, linewidth=0.7))

        # stream data from all files simultaneously
        files = [open(path, 'r') for path in dataset_path]
        for lines in zip(*files):
            for idx, source in enumerate(srcs):
                source.emit(lines[idx])

        # close files
        for file in files:
            file.close()

        yticks_range = range(1, len(dataset_path) + 1)
        plt.yticks(yticks_range, ['Station {0}'.format(idx) for idx in yticks_range])
        if not no_title:
            plt.title('Station with Maximal Hourly Temperature')
        plt.savefig(os.path.join(plot_path, 'index_station_max_temp.png'))
        srcs[0].visualize(os.path.join(plot_path, 'stream_task_2.png'))

    """Task 3 - Implement an algorithm that detects outliers in the temperature data stream."""
    if task == 3:
        if len(dataset_path) > 1:
            raise ValueError('Only a single dataset should be specified for task 3.')

        stream = compute_outliers.get_stream_for_compute_outliers(src)
        stream.sink(Plotter(plot_type='marked-line', unk_val=constants.TEMP_COL_NAME_UNK_VAL_IND, linewidth=0.7))

        # stream data from file
        with open(dataset_path[0], 'r') as f:
            for line in f:
                src.emit(line)

        file_name_stem = pathlib.Path(dataset_path[0]).stem
        plt.ylabel('Temperature in degrees Celsius')
        if not no_title:
            plt.title('Temperatures for {0} with marked outliers'.format(file_name_stem), fontsize=11)
        plt.savefig(os.path.join(plot_path, '{0}_outliers.png'.format(file_name_stem)))
        src.visualize(os.path.join(plot_path, 'stream_task_3.png'))

    """Task 4 - Count the number of the times the temperature in one of the stations is is between -10 and 30 divided in 5. Implement the count-min sketch algorithm."""
    if task == 4:
        if len(dataset_path) > 1:
            raise ValueError('Only a single dataset should be specified for task 4.')

        # unicode characters for infinity and degrees Celsius
        _UNICODE_INF = '\u221e'
        _UNICODE_DEGC = '\u2103'

        (stream_cms, cms), bucket_intervals = compute_count_min_sketch.get_stream_for_compute_count_min_sketch(src, w=w, d=d)
        (stream_exact, exact), _ = compute_count_exact.get_stream_for_compute_count_exact(src)

        # stream data from file
        with open(dataset_path[0], 'r') as f:
            for line in f:
                src.emit(line)

        # value to add to bucket limits to get query values
        add_centering = (bucket_intervals[1] - bucket_intervals[0]) / 2
        x_vals = [bucket_intervals[0] - add_centering] + [val + add_centering for val in bucket_intervals]

        # get counts obtained using the count-min sketch method and the exact counts
        y_vals_cms = [cms.query(x_val) for x_val in x_vals]
        y_vals_exact = [exact.query(x_val) for x_val in x_vals]

        # get plot labels from the bucket intervals
        x_labels = ['-' + _UNICODE_INF + '..' + str(bucket_intervals[0]) + _UNICODE_DEGC] + \
                   [str(bucket_intervals[idx]) + _UNICODE_DEGC + '..' + str(bucket_intervals[idx + 1]) + _UNICODE_DEGC for idx in range(len(bucket_intervals) - 1)] + \
                   [str(bucket_intervals[-1]) + _UNICODE_DEGC + '..' + _UNICODE_INF]

        # plot results
        fig, ax = plt.subplots()
        ax.bar([x - 0.75 for x in x_vals], y_vals_cms, width=1.5, label='Count-min sketch')
        ax.bar([x + 0.75 for x in x_vals], y_vals_exact, width=1.5, label='Exact')
        ax.set_xticks(x_vals, x_labels)
        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right', fontsize=8)
        ax.set_ylabel('Count')
        ax.legend()
        if not no_title:
            ax.set_title('Counts of Bucketed temperatures')
        plt.savefig(os.path.join(plot_path, '{0}_counts.png'.format(pathlib.Path(dataset_path[0]).stem)))
        src.visualize(os.path.join(plot_path, 'stream_task_4.png'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='weather-station-stream-processing')
    parser.add_argument("--task", type=int, default=1, choices=[1, 2, 3, 4], help='task to compute - see README for more information')
    default_datasets_task2 = [
        os.path.join(os.path.dirname(__file__), 'sample-data/CRNS0101-05-2021-AK_Metlakatla_6_S.txt'),
        os.path.join(os.path.dirname(__file__), 'sample-data/CRNS0101-05-2021-AK_Ivotuk_1_NNE.txt'),
        os.path.join(os.path.dirname(__file__), 'sample-data/CRNS0101-05-2021-AK_Gustavus_2_NE.txt')
    ]
    parser.add_argument("--dataset-path", nargs='+', type=str, default=[os.path.join(os.path.dirname(__file__), 'sample-data/CRNS0101-05-2021-AK_Metlakatla_6_S.txt')],
                        help='path to dataset(s) to use')
    parser.add_argument("--plot-dir-path", type=str, default='.', help='path to folder in which to save plots')
    parser.add_argument("--no-title", action='store_true', help='omit title from plots')
    parser.add_argument("--w", type=int, default=4, help='the w parameter for the Min-count sketch algorithm')
    parser.add_argument("--d", type=int, default=5, help='the d parameter for the Min-count sketch algorithm')
    args = parser.parse_args()
    main(args.task, args.dataset_path if args.task != 2 else default_datasets_task2, args.plot_dir_path, args.no_title, args.w, args.d)
