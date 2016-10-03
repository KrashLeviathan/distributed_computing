#!/usr/bin/env python

import pickle

# The worker_clients will write results for their chunk of code
# to a file using the pickle module. Once all those files make
# their way back to the client, they need to be:
#
# - loaded,
# - assembled into a new list in the correct order (because
#    each one will return a list of results), and
# - written to a new file using the pickle module.


def write_data_to_file(file_path, data):
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)


def load_data_from_file(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)
