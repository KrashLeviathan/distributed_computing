#!/usr/bin/python

import threading
import time
import SocketServer
import base64
import socket
import os
from utils import *
from shutil import rmtree

# TODO assign an id to the tasker clients
clients = list()
clients_lock = threading.Lock()
running = True


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
        global running, clients, clients_lock

        # Add client thread
        with clients_lock:
            clients.append(self)

        # Set client_id
        index_of_mem_address = str(self).find("0x")
        self.client_id = str(self)[index_of_mem_address:index_of_mem_address+14]
        self.sendQueue.append("{}{}".format(M_TYPE_CLIENT_ID, self.client_id))

        while running:
            # Send all messages in the sendQueue, clearing the queue afterwards
            for message in self.sendQueue:
                self.request.send(message)
                self._print_traffic(message, sent=True)
            self.sendQueue = []

            # Receive data from the client
            self.data = self._recv_timeout()
            if self.data == M_TYPE_CLOSING:
                print("Closing {} {}".format(self.client_type, self.client_id))
                self.clientRunning = False
                for client in clients:
                    if client is self:
                        clients.remove(client)
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
    Handles requests for Worker clients.
    """

    def __init__(self, request, client_address, server):
        ClientRequestHandler.__init__(self, request, client_address, server, "WorkerClient")

    def handle_by_type(self):
        # TODO: If the message is a set of results, pass it along to the tasker
        pass


class TaskerRequestHandler(ClientRequestHandler):
    """
    Handles requests for Tasker clients.
    """

    def __init__(self, request, client_address, server):
        ClientRequestHandler.__init__(self, request, client_address, server, "TaskerClient")

    def handle_by_type(self):
        zip_file, data_file = self.data.split(MESSAGE_DELIMITER)

        directory = "taskers/tasker_" + self.client_id
        if not os.path.exists(directory):
            os.makedirs(directory)

        # We have collected the entire zip file, now we write it to the servers zip file
        # TODO Change file location and randomly generate file name
        with open(directory + "/calculation.zip", "wb") as fh:
            fh.write(base64.decodestring(zip_file))

        with open(directory + "/data_file.py", "wb") as fh:
            fh.write(base64.decodestring(data_file))

        # server_A.send_to_all(self.data)

        # TODO loop until the results are all done from all the clients.
        # self.request.send("Done! Here are your results!")


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

    @staticmethod
    def kill_clients():
        for client in clients:
            client.request.send(M_TYPE_SHUTDOWN)

    @staticmethod
    def send_to_all(file_data):
        print("Sending to all clients")
        for client in clients:
            client.sendQueue.append(M_TYPE_FILES + file_data)


if __name__ == "__main__":
    # We have two separate servers on two separate ports
    # 9999 listens for worker clients
    # 8888 listens for tasker clients

    HOST = ''
    PORT_A = 9999
    PORT_B = 8888
    try:
        server_A = ThreadedTCPServer((HOST, PORT_A), WorkerRequestHandler)
        server_B = ThreadedTCPServer((HOST, PORT_B), TaskerRequestHandler)

        server_A.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_B.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server_A_thread = threading.Thread(target=server_A.serve_forever)
        server_B_thread = threading.Thread(target=server_B.serve_forever)

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
            server_A.kill_clients()
            server_B.kill_clients()
            server_A.shutdown()
            server_B.shutdown()
            if os.path.exists("taskers"):
                print("Removing \"taskers\" directory...")
                rmtree("taskers")
            print "\n Server Shutdown"

    except socket.error, exc:
        if running:
            print("Failed to start server: {}".format(exc))
