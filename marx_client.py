#!/usr/bin/env python

import socket
from time import sleep, time
from utils import *


# noinspection PyMethodMayBeStatic
class MarxClient:
    def __init__(self, host, port):
        self.clientRunning = True
        self.client_id = ""
        self.sendQueue = []
        self.data = None
        self.host = host
        self.port = port
        # Create a socket (SOCK_STREAM means a TCP socket)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def run(self):
        # Outer loop will keep trying to connect if a connection is lost
        while self.clientRunning:
            try:
                if not self.pre_connect_setup():
                    print("Exiting at pre_connect_setup()")
                    break
                self.sock.connect((self.host, self.port))
                if not self.post_connect_setup():
                    print("Exiting at post_connect_setup()")
                    break

                # Inner loop will handle the open connection
                while self.clientRunning:
                    # Send all messages in the sendQueue, clearing the queue afterwards
                    for message in self.sendQueue:
                        self.sock.send(message)
                        self._print_traffic(message, sent=True)
                    self.sendQueue = []

                    # Receive data from the client
                    try:
                        self.data = self._recv_timeout()
                    except TimeoutException:
                        continue
                    if self.data == M_TYPE_SHUTDOWN:
                        print("\nServer requested shutdown...")
                        self.clientRunning = False
                        break
                    elif self.data == "":
                        print("\nConnection was lost. Shutting down...")
                        self.clientRunning = False
                        break
                    else:
                        self._print_traffic(self.data)
                        successfully_handled = self.handle_message(self.data[:9], self.data[9:])
                        if not successfully_handled:
                            print("DATA NOT HANDLED! Printing data received...\n{}".format(self.data))

                self.teardown()
            except KeyboardInterrupt:
                print("\nShutting down client...")
                self.clientRunning = False
                sleep(1.5)
                self.sock.sendall(M_TYPE_CLOSING)
            except socket.error, exc:
                print("Failure to connect: {}".format(exc))
                self.clientRunning = False
                sleep(1.5)
            finally:
                self.sock.close()
                print("Client Shutdown")

    def send_message(self, m_type, message):
        self.sendQueue.append("{}{}".format(m_type, message))

    def pre_connect_setup(self):
        """
        This method should be implemented for any actions taken before
        the socket connection is made.
        :return: True if successful, otherwise False.
        """
        return True

    def post_connect_setup(self):
        """
        This method should be implemented for any actions taken after
        the socket connection is made, but before the main loop starts.
        :return: True of successful, otherwise False.
        """
        return True

    # noinspection PyUnusedLocal
    def handle_message(self, message_type, message):
        """
        This method should be implemented to handle messages received
        by the client.
        :param message_type: See utils.py for M_TYPE variables
        :param message: The data that comes after the M_TYPE
        :return: True if successful, otherwise False
        """
        return True

    def teardown(self):
        pass

    def _print_traffic(self, message, sent=False):
        formatted_message = message if len(message) < 40 else "{}...".format(message[:37])
        if sent:
            print("[*] Sent:     \"{}\"".format(formatted_message))
        else:
            print("[*] Received: \"{}\"".format(formatted_message))

    # From http://code.activestate.com/recipes/408859/
    # noinspection SpellCheckingInspection
    def _recv_timeout(self, timeout=2):
        self.sock.setblocking(0)
        total_data = []
        begin = time()
        while self.clientRunning:
            # if you got some data, then break after wait sec
            if total_data and time() - begin > timeout:
                break
            # if you got no data at all, wait a little longer
            elif time() - begin > timeout * 2:
                raise TimeoutException("")
            # noinspection PyBroadException
            try:
                data = self.sock.recv(8192).strip()
                if data:
                    total_data.append(data)
                    begin = time()
                else:
                    sleep(0.1)
            except:
                pass
        return ''.join(total_data)
