#!/usr/bin/python

import threading
import time
import SocketServer
import base64
import socket
import random

# TODO assign an id to the tasker clients
clients = list()
clients_lock = threading.Lock()
running = True


class WorkerRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

        # FIXME __init__ isn't getting called til after the clients closes.
        self.clientRunning = True
        self.sendQueue = []
        self.data = None

    def handle(self):

        # FIXME This is a temporary fix, normally the following two lines would go in __init__
        # but for some reason, __init__ is not getting until the clients closes.
        self.clientRunning = True
        self.sendQueue = []

        global running, clients
        self.data = self._recv_timeout()

        with clients_lock:
            clients.append(self)
        print "Worker {} wrote: \"{}\"".format(self, self.data)
        string = "{}".format(self)
        self.request.send(string[42:-1])
        while running and self.clientRunning:
            for message in self.sendQueue:
                print("Sent message from queue")
                self.request.send(message)
            self.sendQueue = []
            self.data = self._recv_timeout()
            if len(self.data) != 0:
                print("Worker {} wrote: \"{}\"".format(self, self.data))
            if self.data == "__CLOSING__":
                self.clientRunning = False
                for client in clients:
                    if client is self:
                        clients.remove(client)

    # From http://code.activestate.com/recipes/408859/
    def _recv_timeout(self, timeout=2):
        self.request.setblocking(0)
        total_data = []
        begin = time.time()
        while 1:
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
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

        # FIXME __init__ isn't getting called til after the clients closes.
        self.clientRunning = True
        self.sendQueue = []

        self.data = None

    def handle(self):

        # FIXME This is a temporary fix, normally the following two lines would go in __init__
        self.clientRunning = True
        self.sendQueue = []

        self.data = self._recv_timeout()
        result = self.data.split("__DATA__")
        zip_file = result[0]
        data_file = result[1]

        # We have collected the entire zip file, now we write it to the servers zip file
        # TODO Change file location and randomly generate file name
        with open("calculation.zip", "wb") as fh:
            fh.write(base64.decodestring(zip_file))

        with open("data_file.py", "wb") as fh:
            fh.write(base64.decodestring(data_file))

        server_A.send_to_all(self.data)

        # TODO loop until the results are all done from all the clients.
        # self.request.send("Done! Here are your results!")

    # From http://code.activestate.com/recipes/408859/
    def _recv_timeout(self, timeout=2):
        self.request.setblocking(0)
        total_data = []
        begin = time.time()
        while 1:
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
    def send_to_all(self, file_data):
        print("Sending to all clients")
        for client in clients:
            client.sendQueue.append("__FILES__" + file_data)
    pass

if __name__ == "__main__":
    # We have two separate servers on two separate ports
    # 9999 listens for worker clients
    # 8888 listens for tasker clients

    HOST = ''
    PORT_A = 9999
    PORT_B = 8888

    while running:
        try:
            server_A = ThreadedTCPServer((HOST, PORT_A), WorkerRequestHandler)
            server_B = ThreadedTCPServer((HOST, PORT_B), TaskerRequestHandler)

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
                server_A.shutdown()
                server_B.shutdown()
                print "\n Server Shutdown"
        except socket.error, exc:
            print("Failed to start server: {}".format(exc))
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                running = False
