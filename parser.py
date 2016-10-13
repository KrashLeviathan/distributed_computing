#!/usr/bin/env python

import base64
import errno
import os
import pickle
import re
from utils import *










def write_data_to_file(file_path, the_data):
    """
    Writes the binary data to a pickle file for transport.
    :param file_path:
    :param the_data:
    :return:
    """
    with open(file_path, 'wb') as f:
        pickle.dump(the_data, f)


class TaskerClient:
    """
        Handles the sending/receiving of information to/from a Tasker Client (the clients
        that requests execution of a piece of code).
        """

    def __init__(self, _id, path_to_zipped_calc_dir, path_to_data_file):
        self.client_id = _id
        # Relative paths to zipped dir, data file, and data chunk files start with:
        #
        # DIRECTORY_PATH + "client_" + self.client_id + "/"
        #
        # Example: /client_files/client_1234/calculation.zip
        #          /client_files/client_1234/data_file.py
        #          /client_files/client_1234/data_1234_1.py
        #          /client_files/client_1234/data_1234_2.py
        #          /client_files/client_1234/data_1234_3.py ... etc
        self.path_to_zipped_calc_dir = path_to_zipped_calc_dir
        self.path_to_data_file = path_to_data_file
        self.chunk_file_paths = []

    def parse_data_file(self):
        """
        Parses the data file whose path is stored in self.path_to_data_file. The data
        file should contain one assignment line for a list structure.
        :return: The invoked data structure from the file.
        """
        with open(self.path_to_data_file, 'r') as data_file:
            assignment_regexp = re.compile(r'^[A-Za-z0-9_]+\s*=\s*\[\s*')
            assignment_line_found = False
            closing_bracket_found = False
            data_string_array = []
            data = None

            for line in data_file:
                if line.startswith("#"):
                    continue
                elif assignment_regexp.search(line) is not None:
                    if not assignment_line_found:
                        assignment_line_found = True
                    else:
                        raise ValueError("Multiple variable assignments discovered in the data file!")
                elif line.rstrip() == "]":
                    # The loop terminates once a closing bracket for the list is found.
                    data = self.invoke_data_structure(data_string_array)
                    closing_bracket_found = True
                elif assignment_line_found and not closing_bracket_found:
                    data_string_array.append(line)
                elif line.strip() == "":
                    continue
                else:
                    raise ValueError("Found line (`" + line + "`) in data file that breaks syntax rules!")

            if assignment_line_found and closing_bracket_found:
                return data
            else:
                raise ValueError("Could not find valid data structure in the data file!\n  assignment_line_found: " +
                                 str(assignment_line_found) + "\n  closing_bracket_found: " +
                                 str(closing_bracket_found))

    def invoke_data_structure(self, data_string_array):
        """
        Executes the lines of code to create the data structure.
        :param data_string_array:
        :return:
        """
        file_path = DIRECTORY_PATH + "client_" + self.client_id + "/temp_data.py"
        make_dirs(file_path)
        with open(file_path, "w") as f:
            f.write("data = [\n")
            for line in data_string_array:
                f.write(line + "\n")
            f.write("]")
        execfile(file_path)
        os.remove(file_path)
        # This variable will exist once execfile runs
        # noinspection PyUnresolvedReferences
        return data

    def write_file_from_chunk(self, chunk, data):
        """
        Writes a portion (chunk) of the data to a pickle file and saves the path to
        the file in the chunk_file_paths instance variable.
        :param chunk:
        :param data:
        :return:
        """
        file_path = DIRECTORY_PATH + "client_" + self.client_id + "/data_" + self.client_id + "_" + chunk.index + ".pkl"
        make_dirs(file_path)
        write_data_to_file(file_path, data[chunk.start:chunk.stop])
        self.chunk_file_paths.append(file_path)


class WorkerClient:
    """
        Handles the sending/receiving of information to/from a Worker Client (the clients
        that executes the code).
        """

    def __init__(self, _id):
        self.client_id = _id
        self.status = M_TYPE_WORKER_AVAILABLE
        self.tasker = None
        self.chunk = None

    def assign(self, tasker_client_id, chunk):
        """
        Assigns this WorkerClient a chunk of code to execute for a given TaskerClient
        :param tasker_client_id: The client_id of the TaskerClient who owns this code
        :param chunk: The range of items from the data set that this worker should complete
        :return:
        """
        self.status = M_TYPE_WORKER_BUSY
        self.tasker = tasker_clients[tasker_client_id]
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
        """
        Handles results received back from the WorkerClient. After it receives them,
        it should pass the results along to the TaskerClient and make itself available
        to the server again.
        :param message: The message received back from the WorkerClient.
        :return:
        """
        self.tasker.complete_chunk(message)
        self.status = M_TYPE_WORKER_AVAILABLE
        self.tasker = None
        self.chunk = None



