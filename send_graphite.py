# example of sending data to a graphite server
# Charlie Schluting <charlie@schluting.com>
import time
import socket
import logging


def send_to_graphite(metric, datavalue, graphite_server, graphite_port):
    """
    Sends metric (arg1) and value (arg2) to graphite.
    """
    message = "%s %s %d\n" % (metric, datavalue, int(time.time()))
    logging.debug("sending to graphite: %s " % message[:-1])

    sock = socket.socket()
    try:
        sock.connect((graphite_server, graphite_port))
        sock.sendall(message)
    except:
        logging.error("%s sending data to graphite failed" % TIMESTAMP)
    sock.close()

