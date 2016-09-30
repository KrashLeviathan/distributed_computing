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


def load_results_from_file(file_path):
    results_file = open(file_path, 'rb')
    results = pickle.load(results_file)
    results_file.close()
    return results


def write_results_to_file(file_path, results):
    results_file = open(file_path, 'wb')
    pickle.dump(results, results_file)
    results_file.close()
