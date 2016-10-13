#!/usr/bin/env python

import errno
import os
from pickle import (dump, load)

# Nine-character message types prepend encoded messages sent over the socket
M_TYPE_CLIENT_ID = "__CLTID__"
M_TYPE_RESULT = "__RESLT__"
M_TYPE_SHUTDOWN = "__SHUTD__"
M_TYPE_FILES = "__FILES__"
M_TYPE_CLOSING = "__CLOSE__"
M_TYPE_WORKER_UNAVAILABLE = "__WKUNV__"
M_TYPE_WORKER_AVAILABLE = "__WKAVL__"
M_TYPE_WORKER_BUSY = "__WKBSY__"

# After data has been encoded for transmission, it gets joined together with
# this string being the delimiter.
MESSAGE_DELIMITER = "__DATA__"


def sort_results(results):
    """Sorts file names based on designated part number.

    File names are expected to be in the form :
        {task_id}.{part number}.{extension}

    :param results: a list of file names to be sorted
    :type results: list of str
    """
    return sorted(results, key=lambda x: x.split('.', maxsplit=2)[1])


def assemble_results(results, out_filename):
    """Assembles multiple files into one.

    :param results: a list of file names to be assembled
    :type results: list of str
    :param out_filename: file the results will be written to
    :type out_filename: str
    """
    for result in results:
        data = load_data_from_file(result)
        write_data_to_file(out_filename, data)


def write_data_to_file(file_path, data):
    with open(file_path, 'wb') as f:
        dump(data, f)


def load_data_from_file(file_path):
    with open(file_path, 'rb') as f:
        return load(f)


class TimeoutException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def make_dirs(file_path):
    """
    Makes the needed directories for the file_path if they don't already exist.
    :param file_path:
    :return:
    """
    if not os.path.exists(os.path.dirname(file_path)):
        try:
            os.makedirs(os.path.dirname(file_path))
        except OSError as os_err:
            if os_err.errno != errno.EEXIST:
                raise
