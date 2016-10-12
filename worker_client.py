#!/usr/bin/env python

import pickle
import socket
import time
import utils
import zipfile

HOST, PORT = "localhost", 9999
running = True
zfile_path = "./calculation.zip"
pklfile_path = "./calculation/main_file.pkl"

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

            # TODO Set vars to received information
            # Save file to zfile_path with data received from sock

            # # Unzip calculation.zip
            # zip_ref = zipfile.ZipFile(zfile_path, 'r')
            # zip_ref.extractall("./")
            # zip_ref.close()
            #
            # # TODO Create the file at pklfile_path from sock
            # utils.write_data_to_file(pklfile_path, received)
            #
            # pkldata = utils.load_data_from_file(pklfile_path)

            # Call calculate in calculation/main_file.py
            # and stage the file to send back to server
            # from main_file import calculate
            # results = calculate(pkldata)
            # print results
            # Send to server
            # sock.sendall(results)

            # Save log as log.txt
            # Check if log.txt exists and send to file
            # if os.path.exists("./log.txt"):
            # Send file data to server
            # sock.sendall()

            # This loop keeps the socket open as long as the server doesnt shut down
            # I HOPE
            while running:
                time.sleep(1)

                # FIXME
                received = _recv_timeout(sock)
                # print("Received: \"{}\"".format(received if len(received) != 0 else ""))

                if received == "":
                    sock.close()
                    break
                elif "__FILES__" in received:
                    print("File Received")

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


# FIXME
# From http://code.activestate.com/recipes/408859/
def _recv_timeout(self, timeout=2):
    self.setblocking(0)
    total_data = []
    begin = time.time()
    while 1:
        # if you got some data, then break after wait sec
        if total_data and time.time() - begin > timeout:
            break
        # if you got no data at all, wait a little longer
        elif time.time() - begin > timeout * 2:
            break
        try:
            data = self.request.recv(8192).strip()
            if data:
                total_data.append(data)
                begin = time.time()
            else:
                time.sleep(0.1)
        except:
            pass
    return ''.join(total_data)

def write_data_to_file(file_path, data):
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)


def load_data_from_file(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)


if __name__ == "__main__":
    main()
