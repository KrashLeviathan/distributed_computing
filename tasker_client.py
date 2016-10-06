#!/usr/bin/env python

import pickle
import socket
import sys
import base64

HOST, PORT = "localhost", 8888

# Create a socket (SOCK_STREAM means a TCP socket)                                       
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Test files for appending
result_files = ['calculation.001.txt', 'calculation.002.txt']
out_file = 'calculation_results.txt'

try:
    if len(sys.argv[1:]) != 2:
        # TODO
        print("USAGE")

    # Connect to server and send data
    sock.connect((HOST, PORT))
    zip_file_name, data_file_name = sys.argv[1:]
    with open(zip_file_name, 'rb') as f:
        encoded = base64.b64encode(f.read())

    sock.sendall(encoded)
    # Receive data from the server and shut down                                         
    received = sock.recv(1024)
finally:
    sock.close()

print "Sent:     {}".format(zip_file_name + ", " + data_file_name)
print "Received: {}".format(received)

# Demonstration of file append
print "Writing to {}".format(out_file)
# Create / clear output file
with open(out_file, 'wb') as f:
    f.write('')
# Write results to the file
for result in result_files:
    # Read in
    with open(result, 'rb') as f:
        data = f.read()
    # Write out
    with open(out_file, 'a') as f:
        f.write(data)

# The worker_clients will write results for their chunk of code
# to a file using the pickle module. Once all those files make
# their way back to the client, they need to be:
#
# - loaded,
# - assembled into a new list in the correct order (because
#    each one will return a list of results), and
# - written to a new file using the pickle module.


def sort_results(results):
    # unusable due to scope, should be moved to some kind of utils file
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
        pickle.dump(data, f)


def load_data_from_file(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)
