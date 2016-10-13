#!/usr/bin/env python

import socket
import sys
import base64
from time import sleep
from utils import *

HOST, PORT = "localhost", 8888
SLEEP_TIME = 2
running = True


def main():
    global running

    if len(sys.argv[1:]) != 2:
        usage()
    zip_file_name, data_file_name = sys.argv[1:]

    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to server and send data
        sock.connect((HOST, PORT))
        encoded = encode_data_from_args(zip_file_name, data_file_name)
        sock.sendall(encoded)
        print "Sent:     {}".format(zip_file_name + ", " + data_file_name)

        # Receive data from the server and shut down
        while running:
            received = sock.recv(1024)
            if not received:
                continue

            message_type = received[:9]
            print "Received: {}".format(received)

            if message_type == M_TYPE_RESULT:
                message = received[9:] if len(received) > 9 else ""
                handle_partial_result(message)
                pass
            elif message_type == M_TYPE_SHUTDOWN:
                print("\nServer requested shutdown...")
                running = False
                break
            elif message_type == "":
                print("\nConnection was lost. Shutting down...")
                running = False
                break
            else:
                print("Received unknown message type: {}".format(message_type))

            sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        print("\nShutting down tasker client...")
        running = False
        sleep(SLEEP_TIME)
        sock.sendall(M_TYPE_CLOSING)
    except socket.error as err:
        print("Socket Error: {}".format(err))
    except IOError as err:
        print("IO Error: {}".format(err))
    finally:
        sock.close()
        print("Tasker Client Shutdown")


def handle_partial_result(result):
    # sort_results(results)
    # assemble_results(results, out_filename)
    # TODO
    print(result)


def encode_data_from_args(zip_file_name, data_file_name):
    with open(zip_file_name, 'rb') as f:
        encoded = base64.b64encode(f.read())
    encoded += MESSAGE_DELIMITER
    with open(data_file_name, 'rb') as f:
        encoded += base64.b64encode(f.read())
    return encoded


def usage():
    # TODO: Write usage statement
    print("USAGE")
    exit()


if __name__ == "__main__":
    main()
