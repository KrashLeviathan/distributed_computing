#!/usr/bin/env python

import pickle
import socket
import sys
import zipfile
import os.path
import utils

HOST, PORT = "localhost", 9999
data = " ".join(sys.argv[1:])
zfile_path = "./calculation.zip"
pklfile_path = "./calculation/main_file.pkl"

WORKER_UNAVAILABLE = 0
WORKER_AVAILABLE = 1
WORKER_BUSY = 2

# Create a socket (SOCK_STREAM means a TCP socket)                                       
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Connect to server and send data                                                    
    #sock.connect((HOST, PORT))
    #sock.sendall("Ready to do work")

    # Receive data from the server and shut down                                         
    received = sock.recv(1024)
	
    # TODO Set vars to received information
	# Save file to zfile_path with data received from sock
	
	# Unzip calculation.zip
	zip_ref = zipfile.ZipFile(zfile_path, 'r')
    zip_ref.extractall("./")
    zip_ref.close()
	
    # TODO Create the file at pklfile_path from sock
    utils.write_data_to_file(pklfile_path, received)
	
    pkldata = utils.load_data_from_file(pklfile_path)
	
    # Call calculate in calculation/main_file.py
	# and stage the file to send back to server
    # from main_file import calculate
    # results = calculate(pkldata)
    # print results
	# Send to server
	# sock.sendall(results)
	
	# Save log as log.txt
	# Check if log.txt exists and send to file
	#if os.path.exists("./log.txt"):
		# Send file data to server
		# sock.sendall()
	
finally:
	print "finally"
#    sock.close()
	# TODO Clean up files

#print "Sent:     {}".format(data)
#print "Received: {}".format(received)


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