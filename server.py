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
        self.data = self._recv_timeout()

        with open("calculation.zip", "wb") as fh:
            fh.write(base64.decodestring(self.data))
        print "Tasker wrote: ", self.data
        self.request.send("Done! Here are your results!")

    # From http://code.activestate.com/recipes/408859/
    def _recv_timeout(self, timeout=2):
        self.request.setblocking(0)
        total_data = []
        data = ''
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
