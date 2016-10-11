#!/usr/bin/env python

import pickle
import socket
import time
import errno
from socket import error as socket_error

HOST, PORT = "localhost", 9999
running = True
WORKER_UNAVAILABLE = 0
WORKER_AVAILABLE = 1
WORKER_BUSY = 2

def main():
    global running

    while running:
        try:
            # Create a socket (SOCK_STREAM means a TCP socket)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Connect to server and send data
            sock.connect((HOST, PORT))
            sock.sendall("Ready to do work")

            # Receive data from the server and shut down
            received = sock.recv(1024)
            print "Sent:     \"Ready to do work\""
            print "Received: \"{}\"".format(received)

            # This loop keeps the socket open as long as the server doesnt shut down
            # I HOPE
            while running:
                time.sleep(1)
                received = sock.recv(1024)
                print("Received: \"{}\"".format(received if len(received) != 0 else ""))
                if received == "":
                    sock.close()
                    break
        except KeyboardInterrupt:
            print("\nShutting down worker client...")
            running = False
            time.sleep(1.5)
            sock.sendall("__CLOSING__")
            sock.close()
            print("WorkerClient Shutdown")
        except socket.error, exc:
            print("Failure to connect: {}".format(exc))
            time.sleep(1)
        finally:
            pass


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

if __name__ == "__main__":
    main()
