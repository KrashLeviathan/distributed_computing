#!/usr/bin/python

import threading
import time
import SocketServer

class WorkerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        self.data = self.request.recv(1024).strip()
        for i in range(0,5):
            print i
            time.sleep(1)

        print "Worker:\n"
        print "%s wrote: " % self.client_address[0]
        print self.data
        self.request.send(self.data.upper())

class TaskerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        self.data = self.request.recv(1024).strip()
        for i in range(0,5):
            print i
            time.sleep(1)

        print "Tasker:\n"
        print "%s wrote: " % self.client_address[0]
        print self.data
        self.request.send(self.data.upper())


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

    while 1:
        time.sleep(1)
