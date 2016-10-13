#!/usr/bin/python

import threading
import time
import SocketServer
import base64
import socket
import os
from utils import *

# TODO assign an id to the tasker clients
clients = list()
clients_lock = threading.Lock()
running = True


class WorkerRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.clientRunning = True
        self.sendQueue = []
        self.data = None

        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):

        global running, clients
        self.data = self._recv_timeout()

        with clients_lock:
            clients.append(self)
        print "Worker {} wrote: \"{}\"".format(self, self.data)
        string = "{}".format(self)
        self.request.send(string[42:-1])
        while running:
            for message in self.sendQueue:
                print("Sent message from queue")
                self.request.send(message)
            self.sendQueue = []
            self.data = self._recv_timeout()
            if len(self.data) != 0:
                print("Worker {} wrote: \"{}\"".format(self, self.data))
            if self.data == M_TYPE_CLOSING:
                print("closing worker")
                self.clientRunning = False
                for client in clients:
                    if client is self:
                        clients.remove(client)
                break

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


class TaskerRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.clientRunning = True
        self.sendQueue = []

        self.data = None
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        with clients_lock:
            clients.append(self)

        self.data = self._recv_timeout()
        result = self.data.split(MESSAGE_DELIMITER)
        zip_file = result[0]
        data_file = result[1]
        id = "{}".format(self)
        print self
        directory = "tasker_" + id[43:-1]
        if not os.path.exists(directory):
            os.makedirs("taskers/" + directory)

        # We have collected the entire zip file, now we write it to the servers zip file
        # TODO Change file location and randomly generate file name
        with open("taskers/" + directory + "/calculation.zip", "wb") as fh:
            fh.write(base64.decodestring(zip_file))

        with open("taskers/" + directory + "/data_file.py", "wb") as fh:
            fh.write(base64.decodestring(data_file))

        # server_A.send_to_all(self.data)

        # TODO loop until the results are all done from all the clients.
        # self.request.send("Done! Here are your results!")

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
            print "\n Server Shutdown"

    except socket.error, exc:
        if running:
            print("Failed to start server: {}".format(exc))
