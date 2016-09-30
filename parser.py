#!/usr/bin/env python

import base64
import re


tasker_clients = []
worker_clients = []
available_workers = 0  # TODO: Increment and decrement as needed (Charlie)


def handle_client_data(tasker_client):
    """
    This method gets called after a TaskerClient has connected and sent data over
    the network. The data should already be saved to the server and the
    appropriate paths saved inside the TaskerClient at this point.
    :param tasker_client: A TaskerClient
    :return:
    """
    tasker_client.parse_data_file()
    # TODO: Send zipped calc dir and data_portion_file to Worker Clients


class TaskerClient:
    def __init__(self, _id, path_to_zipped_calc_dir, path_to_data_file):
        self._id = _id
        self.path_to_zipped_calc_dir = path_to_zipped_calc_dir
        self.path_to_data_file = path_to_data_file
        self.chunk_paths = []

    def parse_data_file(self):
        data_file = open(self.path_to_data_file, 'r')
        assignment_regexp = re.compile(r'')
        data_string_array = []

        for line in data_file:
            if line.startswith("#"):
                continue
            elif assignment_regexp.search(line) is not None:
                data_string_array.append(line.strip())
            elif line.strip() == "]":
                data_string_array.append("]")
                break
            elif len(data_string_array) != 0:
                data_string_array.append(line)
            elif line.strip() == "":
                continue
            else:
                raise ValueError("Found line (`" + line + "`) in data_file.py that breaks syntax rules!")
        # TODO: Left off working here!

    def write_file_from_chunk(self, file_path, chunk):
        # TODO
        file = open(file_path, 'w')
        file.write(chunk)

    def complete_chunk(self, message):
        print()
        # TODO: Send results back over the network to Tasker Client


class WorkerClient:
    def __init__(self, _id):
        self._id = _id
        self.available = True
        self.tasker = None
        self.chunk_number = 0

    def assign(self, client_id, chunk_number):
        self.available = False
        self.tasker = tasker_clients[client_id]
        self.chunk_number = chunk_number
        with open(self.tasker.path_to_zipped_calc_dir) as zip_file:
            encoded_zip_file = base64.b64encode(zip_file.readlines())
            self.send(encoded_zip_file)
        with open(self.tasker.chunk_paths[chunk_number]) as data_file:
            encoded_data_file = base64.b64encode(data_file.readlines())
            self.send(encoded_data_file)

    def send(self, message):
        # TODO: Send stuff over network to worker client (Charlie)
        print("TODO")

    def on_received_results(self, message):
        self.tasker.complete_chunk(message)
        self.available = True
        self.tasker = None
        self.chunk_number = 0
