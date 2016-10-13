#!/usr/bin/env python

import socket
import sys
import base64
from time import sleep

HOST, PORT = "localhost", 8888

# Create a socket (SOCK_STREAM means a TCP socket)                                       
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    if len(sys.argv[1:]) != 2:
        # TODO
        print("USAGE")

    # Connect to server and send data
    sock.connect((HOST, PORT))
    zip_file_name, data_file_name = sys.argv[1:]
    with open(zip_file_name, 'rb') as f:
        encoded = base64.b64encode(f.read())
    encoded += "__DATA__"
    with open(data_file_name, 'rb') as f:
        encoded += base64.b64encode(f.read())

    sock.sendall(encoded)
    # Receive data from the server and shut down
    while True:
        received = sock.recv(1024)
        message_type = received[:9]

        if received:
            print('got something')

        if message_type == "__SHUTD__":
            print("\nServer requested shutdown...")
            sock.close()
            print("Tasker Client Shutdown")
            break
        sleep(1)
except:
    sock.close()
finally:
    sock.close()

print "Sent:     {}".format(zip_file_name + ", " + data_file_name)
print "Received: {}".format(received)

# The worker_clients will write results for their chunk of code
# to a file using the pickle module. Once all those files make
# their way back to the clients, they need to be:
#
# - loaded,
# - assembled into a new list in the correct order (because
#    each one will return a list of results), and
# - written to a new file using the pickle module.
