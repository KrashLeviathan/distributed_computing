#!/usr/bin/env python

import pickle
import socket
import sys
import zipfile

HOST, PORT = "localhost", 9999
data = " ".join(sys.argv[1:])
zfile_path = "./calculation.zip"
pklfile_path = "./*.pkl"

# Create a socket (SOCK_STREAM means a TCP socket)                                       
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Connect to server and send data                                                    
    sock.connect((HOST, PORT))
    sock.sendall("Ready to do work")

    # Receive data from the server and shut down                                         
    received = sock.recv(1024)
	
	# TODO Set vars to received information
	# Save file to zfile_path from sock
	
	# Unzip calculation.zip
	zipfile.extractall(zfile_path)
	
	# TODO Create the file at pklfile_path from sock
	# write_data_to_file(pklfile_path, received)
	
	# pkldata = load_data_from_file(pklfile_path)
	
	# Call calculate in calculation/main_file.py
	# and stage the file to send back to server
	
	# Send to server
	
finally:
    sock.close()

print "Sent:     {}".format(data)
print "Received: {}".format(received)



WORKER_UNAVAILABLE = 0
WORKER_AVAILABLE = 1
WORKER_BUSY = 2


def write_data_to_file(file_path, data):
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)


def load_data_from_file(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)


# Brody,
#
# I'll be sending each worker two things:
#
# 1) the zipped calculation directory named `calculation.zip`, and
# 2) a file called `data_<client_id>_<chunk_serial_#>.pkl`.
#
# You'll need to:
#
# - Unzip the calculation directory.
# - Load the data set from the pkl file using load_data_from_file()
# - Run the calculate() method in `calculation/main_file.py` with data
# - Use the write_data_to_file() method to write the results to a file
#     (Maybe use `results_<client_id>_<chunk_serial_#>.pkl as filename?)
# - Use Charlie's API to send the results file and a log file (similar naming)
#   back to the server
