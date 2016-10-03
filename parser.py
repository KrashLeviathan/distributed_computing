#!/usr/bin/env python

import base64
import re

WORKER_UNAVAILABLE = 0
WORKER_AVAILABLE = 1
WORKER_BUSY = 2

tasker_clients = []
worker_clients = []


def get_available_workers():
    return [worker for worker in worker_clients if worker.status == WORKER_AVAILABLE]


def handle_client_data(tasker_client):
    """
    This method gets called after a TaskerClient has connected and sent data over
    the network. The data should already be saved to the server and the
    appropriate paths saved inside the TaskerClient at this point.
    :param tasker_client: A TaskerClient
    :return:
    """
    assignment_line, data_string_array = tasker_client.parse_data_file()
    workers_for_task = get_available_workers()
    chunks = []
    for i in range(len(workers_for_task)):
        chunk = Chunk(
            len(data_string_array),
            len(workers_for_task),
            i
        )
        chunks.append(chunk)
        # TODO: Write file
        workers_for_task[i].assign(tasker_client._id, chunk)
    # TODO: Send zipped calc dir and data_portion_file to Worker Clients


class TaskerClient:
    """
    Handles the sending/receiving of information to/from a Tasker Client (the client
    that requests execution of a piece of code).
    """
    def __init__(self, _id, path_to_zipped_calc_dir, path_to_data_file):
        self.client_id = _id
        self.path_to_zipped_calc_dir = path_to_zipped_calc_dir
        self.path_to_data_file = path_to_data_file
        self.chunk_paths = []

    def parse_data_file(self):
        data_file = open(self.path_to_data_file, 'r')
        assignment_regexp = re.compile(r'^[A-Za-z0-9_]+\s*=\s*\[\s*')
        assignment_line = None
        data_string_array = []

        for line in data_file:
            if line.startswith("#"):
                continue
            elif assignment_regexp.search(line) is not None:
                assignment_line = line
            elif line.rstrip() == "]":
                # The loop terminates once a closing bracket for the list is found.
                break
            elif assignment_line is not None:
                data_string_array.append(line)
            elif line.strip() == "":
                continue
            else:
                raise ValueError("Found line (`" + line + "`) in data_file.py that breaks syntax rules!")

        return assignment_line, data_string_array

    def write_file_from_chunk(self, file_path, chunk):
        # TODO
        chunk_file = open(file_path, 'w')
        chunk_file.write(chunk)

    def complete_chunk(self, message):
        print()
        # TODO: Send results back over the network to Tasker Client


class WorkerClient:
    """
    Handles the sending/receiving of information to/from a Worker Client (the client
    that executes the code).
    """
    def __init__(self, _id):
        self.client_id = _id
        self.status = WORKER_AVAILABLE
        self.tasker = None
        self.chunk = None

    def assign(self, client_id, chunk):
        self.status = WORKER_BUSY
        self.tasker = tasker_clients[client_id]
        self.chunk = chunk
        with open(self.tasker.path_to_zipped_calc_dir) as zip_file:
            encoded_zip_file = base64.b64encode(zip_file.readlines())
            self.send(encoded_zip_file)
        with open(self.tasker.chunk_paths[chunk.index]) as data_file:
            encoded_data_file = base64.b64encode(data_file.readlines())
            self.send(encoded_data_file)

    def send(self, message):
        # TODO: Send stuff over network to worker client (Charlie)
        print("TODO")

    def on_received_results(self, message):
        self.tasker.complete_chunk(message)
        self.status = WORKER_AVAILABLE
        self.tasker = None
        self.chunk = None


class Chunk:
    """
    Defines the range (chunk) of code that is to be executed by a WorkerClient
    """
    def __init__(self, total_count, divisions, div_index):
        self.index = div_index
        quotient = total_count / divisions
        self.start = quotient * div_index
        # Last chunk index gets the remainder of items
        count = quotient if (div_index != divisions - 1) else quotient + (total_count % divisions)
        self.stop = self.start + count

    def get_range(self):
        return range(self.start, self.stop)

    def get_count(self):
        return self.stop - self.start
