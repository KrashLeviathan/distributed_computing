#!/usr/bin/env python

import base64
from utils import *
import zipfile
import os
import marx_client
from shutil import rmtree

HOST, PORT = "localhost", 9999
zfile_path = "./calculation.zip"
pklfile_path = "./calculation/main_file.pkl"


class WorkerClient(marx_client.MarxClient):
    def __init__(self):
        marx_client.MarxClient.__init__(self, HOST, PORT)

    def pre_connect_setup(self):
        pass

    def post_connect_setup(self):
        pass

    def handle_message(self, message_type, message):
        if message_type == M_TYPE_CLIENT_ID:
            self.client_id = message
            self.sendQueue.append(M_TYPE_WORKER_AVAILABLE)
            return True
        elif message_type == M_TYPE_FILES:
            return self.handle_files(message)
        else:
            return False

    def teardown(self):
        # Remove clients folder if the worker is shutdown
        if os.path.exists("clients"):
            rmtree("clients")

    def handle_files(self, message):
        print("Files Received")  # FIXME: Remove

        zip_file, data_file = message.split(MESSAGE_DELIMITER)
        directory = "client_" + self.client_id
        os.makedirs("clients/" + directory)

        with open("clients/" + directory + "/calculation.zip", "wb") as fh:
            fh.write(base64.decodestring(zip_file))
        with open("clients/" + directory + "/data_file.py", "wb") as fh:
            fh.write(base64.decodestring(data_file))

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

        return True


if __name__ == "__main__":
    WorkerClient().run()
