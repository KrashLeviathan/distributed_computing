#!/usr/bin/env python

import base64
import imp
from utils import *
import zipfile
import os
import marx_client
from shutil import rmtree

HOST, PORT = "localhost", 9999
zip_file_name = "calculation.zip"
data_file_name = "data_file.py"


class WorkerClient(marx_client.MarxClient):
    def __init__(self):
        marx_client.MarxClient.__init__(self, HOST, PORT)

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
        base_client_directory = self._save_files_to_clients(message)

        # Load the data from data_file.py
        data_module = imp.load_source('module.name', base_client_directory + "/" + data_file_name)
        print(data_module.data)

        # Call calculate in calculation/main_file.py
        # and stage the file to send back to server
        # noinspection PyUnresolvedReferences
        from clients.client_.calculation.main_file import calculate
        results = calculate(data_module.data)
        # Send to server
        self.send_message(M_TYPE_RESULT, results)

        # Save log as log.txt
        # Check if log.txt exists and send to file
        # TODO: Log file can be done in future iterations, but not this one!
        # if os.path.exists(base_client_directory + "./log.txt"):
        #     # Send file data to server
        #     with open("example_1/calculation.zip", 'rb') as f:
        #         encoded = base64.b64encode(f.read())
        #     self.send_message(M_TYPE_LOG, logfile)

        return True

    def _save_files_to_clients(self, message):
        # Split
        zip_file, data_file = message.split(MESSAGE_DELIMITER)

        # Create directories, complete with __init__.py files to make them modules
        base_client_directory = "clients/client_" + self.client_id
        if not os.path.exists(base_client_directory):
            os.makedirs(base_client_directory)
        open('clients/__init__.py', 'a').close()
        open(base_client_directory + "/__init__.py", 'a').close()

        # Save the zip and data files
        with open(base_client_directory + "/" + zip_file_name, "wb") as fh:
            fh.write(base64.decodestring(zip_file))
        with open(base_client_directory + "/" + data_file_name, "wb") as fh:
            fh.write(base64.decodestring(data_file))

        # Unzip calculation.zip
        zip_ref = zipfile.ZipFile(base_client_directory + "/" + zip_file_name, 'r')
        zip_ref.extractall(base_client_directory + "/")
        zip_ref.close()

        # If the calculation directory isn't already a module, make it one
        open(base_client_directory + "/calculation/__init__.py", 'a').close()

        return base_client_directory


if __name__ == "__main__":
    WorkerClient().run()

    # # UNCOMMENT for testing!
    # with open("example_1/calculation.zip", 'rb') as f:
    #     encoded = base64.b64encode(f.read())
    # encoded += MESSAGE_DELIMITER
    # with open("example_1/data_file.py", 'rb') as f:
    #     encoded += base64.b64encode(f.read())
    # WorkerClient().handle_files(encoded)
