#!/usr/bin/python

import threading
import time
import SocketServer
import base64
import socket

# TODO assign an id to the tasker client

clients = list()

clients_lock = threading.Lock()

running = True

class WorkerRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)
        self.clientRunning = True
        self.sendQueue = []
        self.data = None

    def handle(self):
        global running, clients
        self.clientRunning = True
        self.sendQueue = []
        self.data = self._recv_timeout()
        with clients_lock:
            clients.append(self)
        print "Worker {} wrote: \"{}\"".format(self, self.data)
        self.request.send("Execute This code real quick.")
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
        self.clientRunning = True
        self.sendQueue = []
        self.data = None

    def handle(self):
        self.data = self._recv_timeout()
        # We have collected the entire zip file, now we write it to the servers zip file
        # TODO Change file location and randomly generate file name
        with open("calculation.zip", "wb") as fh:
            fh.write(base64.decodestring(self.data))

        server_A.send_to_all('calculation.zip')

        self.request.send("Done! Here are your results!")

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
    def send_to_all(self, message):
        print("Sending to all clients")
        zip_file_name = message
        with open(zip_file_name, 'rb') as f:
            encoded = base64.b64encode(f.read())
        for client in clients:
            client.sendQueue.append("__FILES__" + encoded)

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
