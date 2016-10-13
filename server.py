#!/usr/bin/python

import threading
import time
import SocketServer
import base64
import socket
import re
from utils import *
from shutil import rmtree

# TODO assign an id to the tasker clients
clients_lock = threading.Lock()
running = True

DIRECTORY_PATH = "/client_files/"

tasker_clients = []
worker_clients = []


def get_available_workers():
    return [worker for worker in worker_clients if worker.status == M_TYPE_WORKER_AVAILABLE]


def add_client(client):
    with clients_lock:
        if client.client_type == "WorkerClient":
            worker_clients.append(client)
        else:
            tasker_clients.append(client)


def remove_client(client):
    if client.client_type == "WorkerClient":
        worker_clients.remove(client)
    else:
        tasker_clients.remove(client)


class ClientRequestHandler(SocketServer.BaseRequestHandler):
    """
    Generic client request handler. Gets extended by the WorkerRequestHandler
    and TaskerRequestHandler classes. Any messages added to the sendQueue will
    be sent to the client. Any messages received by the server from
    this socket will be handled via the handle_by_type() method implemented
    by the concrete classes.
    """

    def __init__(self, request, client_address, server, client_type):
        self.clientRunning = True
        self.client_id = ""
        self.sendQueue = []
        self.data = None
        self.client_type = client_type
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        global running

        # Set client_id
        index_of_mem_address = str(self).find("0x")
        self.client_id = str(self)[index_of_mem_address:index_of_mem_address+14]
        print("[*] Connected {} {}.".format(self.client_type, self.client_id))
        self.send_message(M_TYPE_CLIENT_ID, self.client_id)

        while running:
            # Send all messages in the sendQueue, clearing the queue afterwards
            for message in self.sendQueue:
                self.request.send(message)
                self._print_traffic(message, sent=True)
            self.sendQueue = []

            # Receive data from the client
            self.data = self._recv_timeout()
            if self.data == M_TYPE_CLOSING:
                print("[*] Disconnected {} {}.".format(self.client_type, self.client_id))
                self.clientRunning = False
                remove_client(self)
                break
            elif len(self.data) != 0:
                self._print_traffic(self.data)
                self.handle_by_type()

    def handle_by_type(self):
        """
        The data handling to be implemented by the concrete classes (client types).
        :return:
        """
        pass

    def send_message(self, m_type, message):
        self.sendQueue.append("{}{}".format(m_type, message))

    def _print_traffic(self, message, sent=False):
        formatted_message = message if len(message) < 40 else "{}...".format(message[:37])
        if sent:
            print("[*] Sent to       {} {}: \"{}\"".format(self.client_type, self.client_id, formatted_message))
        else:
            print("[*] Received from {} {}: \"{}\"".format(self.client_type, self.client_id, formatted_message))

    # From http://code.activestate.com/recipes/408859/
    def _recv_timeout(self, timeout=2):
        self.request.setblocking(0)
        total_data = []
        begin = time.time()
        while running:
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


class WorkerRequestHandler(ClientRequestHandler):
    """
    Handles the sending/receiving of information to/from a Worker Client (the clients
    that executes the code).
    """
    def __init__(self, request, client_address, server):
        self.client_id = ""
        self.status = M_TYPE_WORKER_AVAILABLE
        self.tasker = None
        self.chunk = None
        ClientRequestHandler.__init__(self, request, client_address, server, "WorkerClient")

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
        with open(self.tasker.chunk_paths[chunk.index]) as data_file:
            encoded_data_file = base64.b64encode(data_file.readlines())
        encoded = encoded_zip_file + MESSAGE_DELIMITER + encoded_data_file
        self.send_message(M_TYPE_FILES, encoded)

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

    def handle_by_type(self):
        if self.data[:9] == M_TYPE_WORKER_AVAILABLE:
            pass
        # TODO: If the message is a set of results, pass it along to the tasker
        if self.data[:9] == M_TYPE_RESULT:
            self.on_received_results(self.data[9:])


class TaskerRequestHandler(ClientRequestHandler):
    """
    Handles the sending/receiving of information to/from a Tasker Client (the clients
    that requests execution of a piece of code).
    """

    def __init__(self, request, client_address, server):
        self.client_id = ""
        # Relative paths to zipped dir, data file, and data chunk files start with:
        #
        # DIRECTORY_PATH + "client_" + self.client_id + "/"
        #
        # Example: /client_files/client_1234/calculation.zip
        #          /client_files/client_1234/data_file.py
        #          /client_files/client_1234/data_1234_1.py
        #          /client_files/client_1234/data_1234_2.py
        #          /client_files/client_1234/data_1234_3.py ... etc
        self.path_to_zipped_calc_dir = ""
        self.path_to_data_file = ""
        self.chunk_file_paths = []
        ClientRequestHandler.__init__(self, request, client_address, server, "TaskerClient")

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

    def handle_client_data(self):
        """
        This method gets called after a TaskerClient has connected and sent data over
        the network. The data should already be saved to the server and the
        appropriate paths saved inside the TaskerClient at this point.
        :return:
        """
        assignment_line, data_string_array = self.parse_data_file()
        workers_for_task = get_available_workers()
        for i in range(len(workers_for_task)):
            chunk = Chunk(
                len(data_string_array),
                len(workers_for_task),
                i
            )
            self.write_file_from_chunk(chunk, data_string_array)
            workers_for_task[i].assign(self.client_id, chunk)

    def handle_by_type(self):
        zip_file, data_file = self.data.split(MESSAGE_DELIMITER)

        directory = "taskers/tasker_" + self.client_id
        if not os.path.exists(directory):
            os.makedirs(directory)

        # We have collected the entire zip file, now we write it to the servers zip file
        # TODO Change file location and randomly generate file name
        self.path_to_zipped_calc_dir = directory + "/calculation.zip"
        self.path_to_data_file = directory + "/data_file.py"
        with open(self.path_to_zipped_calc_dir, "wb") as fh:
            fh.write(base64.decodestring(zip_file))
        with open(self.path_to_data_file, "wb") as fh:
            fh.write(base64.decodestring(data_file))

        self.handle_client_data()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    @staticmethod
    def kill_clients():
        for client in tasker_clients + worker_clients:
            client.request.send(M_TYPE_SHUTDOWN)

    @staticmethod
    def send_to_all(file_data):
        print("Sending to all clients")
        for client in tasker_clients + worker_clients:
            client.send_message(M_TYPE_FILES, file_data)


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


if __name__ == "__main__":
    # We have two separate servers on two separate ports
    # 9999 listens for worker clients
    # 8888 listens for tasker clients

    HOST = ''
    PORT_A = 9999
    PORT_B = 8888
    try:
        worker_server = ThreadedTCPServer((HOST, PORT_A), WorkerRequestHandler)
        tasker_server = ThreadedTCPServer((HOST, PORT_B), TaskerRequestHandler)

        worker_server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tasker_server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server_A_thread = threading.Thread(target=worker_server.serve_forever)
        server_B_thread = threading.Thread(target=tasker_server.serve_forever)

        server_A_thread.setDaemon(True)
        server_B_thread.setDaemon(True)

        server_A_thread.start()
        server_B_thread.start()
        print "Listening for Taskers on Port 8888"
        print "Listening for Workers on Port 9999"
        try:
            while running:
                time.sleep(1)
        except KeyboardInterrupt:
            print "\n Shutting down server..."
            running = False
        finally:
            worker_server.kill_clients()
            tasker_server.kill_clients()
            worker_server.shutdown()
            tasker_server.shutdown()
            if os.path.exists("taskers"):
                print("Removing \"taskers\" directory...")
                rmtree("taskers")
            print "\n Server Shutdown"

    except socket.error, exc:
        if running:
            print("Failed to start server: {}".format(exc))
