#!/usr/bin/env python

import base64
from utils import *
import zipfile
import os
import sys
import marx_client
from shutil import rmtree

HOST, PORT = "localhost", 8888
zfile_path = "./calculation.zip"
pklfile_path = "./calculation/main_file.pkl"


class TaskerClient(marx_client.MarxClient):
    def __init__(self):
        self.zip_file_name = ""
        self.data_file_name = ""
        marx_client.MarxClient.__init__(self, HOST, PORT)

    def pre_connect_setup(self):
        if len(sys.argv[1:]) != 2:
            usage()
            return False
        self.zip_file_name, self.data_file_name = sys.argv[1:]
        return True

    def handle_message(self, message_type, message):
        if message_type == M_TYPE_CLIENT_ID:
            self.client_id = message
            try:
                encoded = encode_data_from_args(self.zip_file_name, self.data_file_name)
                self.send_message(M_TYPE_FILES, encoded)
                return True
            except IOError as err:
                print("IO Error: {}".format(err))
                return False
        elif message_type == M_TYPE_RESULT:
            return self.handle_result(message)
        else:
            return False

    def handle_result(self, result):
        # sort_results(results)
        # assemble_results(results, out_filename)
        # TODO
        print(result)
        return True


def encode_data_from_args(zip_file_name, data_file_name):
    with open(zip_file_name, 'rb') as f:
        encoded = base64.b64encode(f.read())
    encoded += MESSAGE_DELIMITER
    with open(data_file_name, 'rb') as f:
        encoded += base64.b64encode(f.read())
    return encoded


def usage():
    # TODO: Write usage statement
    print("USAGE")
    exit()


if __name__ == "__main__":
    TaskerClient().run()
