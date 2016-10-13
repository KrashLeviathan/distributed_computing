#!/usr/bin/env python

import socket
import time
import base64
from utils import *
import zipfile
import os
import marx_client

HOST, PORT = "localhost", 9999
running = True
zfile_path = "./calculation.zip"
pklfile_path = "./calculation/main_file.pkl"
client_id = 0


class WorkerClient(marx_client.MarxClient):
    def __init__(self):
        marx_client.MarxClient.__init__(self, HOST, PORT)

    def pre_connect_setup(self):
        pass

    def post_connect_setup(self):
        pass

    def handle_message(self, message_type, message):
        if message_type == M_TYPE_FILES:
            print("File Received") # FIXME: Remove

            zip_file, data_file = message.split(MESSAGE_DELIMITER)
            directory = "client_" + self.client_id
            os.makedirs("clients/" + directory)

            with open("clients/" + directory + "/calculation.zip", "wb") as fh:
                fh.write(base64.decodestring(zip_file))
            with open("clients/" + directory + "/data_file.py", "wb") as fh:
                fh.write(base64.decodestring(data_file))

            return True
        else:
            return False

    def teardown(self):
        pass


if __name__ == "__main__":
    WorkerClient().run()
