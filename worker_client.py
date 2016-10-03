#!/usr/bin/env python

import pickle


WORKER_UNAVAILABLE = 0
WORKER_AVAILABLE = 1
WORKER_BUSY = 2


def write_results_to_file(file_path, results):
    results_file = open(file_path, 'wb')
    pickle.dump(results, results_file)
    results_file.close()


# Brody,
#
# I'll be sending each worker two things:
#
# 1) the zipped calculation directory named `calculation.zip`, and
# 2) a file called `data_<client_id>_<chunk_serial_#>.py`.
#
# You'll need to:
#
# - Unzip the calculation directory.
# - Run the calculate() method in `calculation/main_file.py` with the
#   data set contained in the data file
# - Use the write_results_to_file() method to write the results to a file
#     (Maybe use `results_<client_id>_<chunk_serial_#>.pkl as filename?)
# - Use Charlie's API to send the results file and a log file (similar naming)
#   back to the server
# -
