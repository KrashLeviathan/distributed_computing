#!/usr/bin/python

import threading
import time
import SocketServer
import base64
import signal
import sys


class WorkerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        self.data = self.request.recv(1024).strip()
        print "Worker {} wrote: {}".format(self.client_address[0], self.data)
        self.request.send("Execute This code real quick.")


class TaskerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        self.data = self.request.recv(1024).strip()
        # or, more concisely using with statement
        file = bytes([self.data])
        with open("calculation.zip", "wb") as fh:
            fh.write(base64.b64decode(file))
        print "Tasker wrote: ", self.data
        self.request.send("Done! Here are your results!")


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == "__main__":

    HOST = ''
    PORT_A = 9999
    PORT_B = 8888

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
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print "\n Shutting down server..."
        server_A.shutdown()
        server_B.shutdown()
        print "\n Server Shutdown"
