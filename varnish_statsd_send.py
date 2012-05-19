#!/usr/bin/env python
#
# Charlie Schluting <charlie@schluting.com>
#
### Running: ./varnish_statsd_send.py /path/to/logfile [--stdin] [--cluster] [--environment]
###
### Use inotify to tail logfile (argv[1]) for varnish data which we'll
### assume has the following format:
#   varnishncsa -F '%U %{Varnish:time_firstbyte}x %{X-Request-Backend}o %{Varnish:hitmiss}x'
###   e.g. "/path/foo.gif 0.000345 foo hit"
###
### Can also be used to log HTTP response codes, but we currently do that by other means.

import sys
import os
import pyinotify
import kruxstatsd
from optparse       import OptionParser
from pprint         import PrettyPrinter


# function to take input from varnish, and parse into request_time and
# /path (top-level only)
def convert_input_to_keys(data):

    # checking for malformed data
    if len(data) < 4:
        sys.stderr.write("malformed line: %s\n" % data)
        return(None)

    # unpack the data
    path, request_time, backend, hitmiss = data

    # we set X-Request-Backend to 'nonematch' if there was no backend (or synthetic response) to send it to
    # this avoids creating infinite metrics in graphite for /randomURIs that hit us.
    if 'nonematch' in backend:
        path = 'catchall'

    # clean up the path
    path = path.rstrip('/').lstrip('/').replace('.', '_')

    if '/' in path:
        parts = path.split('/')
        endpoint_top_level = parts[0]
    else:
        endpoint_top_level = path

    request_time = int(float(request_time) * 1000000)

    return request_time, endpoint_top_level, hitmiss

# function to call, which shoves the stat in statsd via the kruxstatsd library.
def run(varnish_says, k):

    data = None
    if not varnish_says:
        sys.stdout.write("got to run(), with no data!\n")
    else:
        data = convert_input_to_keys(varnish_says.split())

    if not data:
        return

    request_time, endpoint_top_level, hitmiss = data

    k.timing(endpoint_top_level + '.request_time', request_time)
    k.incr(endpoint_top_level + '.' + hitmiss)         # log a counter for req/s, too

    k.incr("_total_cluster_requests") # also, log a hit for cluster-level req/s!
    return

if __name__ == '__main__':

    # parse some options!
    parser = OptionParser()
    parser.add_option( "--cluster",     dest="cluster_name", default="ungrouped",
                        help="The cluster_name stats will be grouped in" ),
    parser.add_option( "--environment", dest="environment",
                        default=os.environ.get('KRUX_ENVIRONMENT', 'prototype'),
                        help="The (krux) environment the script is running in" ),
    parser.add_option("--stdin",        dest="stdin",        action="store_true",
                        help="Instead of tailing a log file, read from stdin",)
    (options, args) = parser.parse_args()

    # this defines the graphite metric. e.g. timers.httpd.logger-b....
    stat_name = 'httpd.' + options.cluster_name
    k         = kruxstatsd.StatsClient(stat_name, env=options.environment)

    # now, read what varnish sent from stdin, forever:
    if options.stdin:
        for varnish_says in sys.stdin:
            run(varnish_says, k)

    # else, from a file using inotify, if --stdin wasn't used.
    else:
        myfile = args[0]
        print "I am totally watching " + myfile

        wm = pyinotify.WatchManager()

        # watched events on the directory, and parse $path for file_of_interest:
        dirmask = pyinotify.IN_MODIFY | pyinotify.IN_DELETE | pyinotify.IN_MOVE_SELF | pyinotify.IN_CREATE

        # open file, skip to end.. global, because we need it within the below
        # class, every time it's triggered.
        global fh
        try:
            fh = open(myfile, 'r')
        except:
            sys.stderr.write("unable to open file %s" % myfile)
        fh.seek(0,2)

        # the event handlers:
        class EventHandler(pyinotify.ProcessEvent):
            def __init__(self):
                self.last_position_in_file = 0
                pyinotify.ProcessEvent.__init__(self)

            def process_IN_MODIFY(self, event):

                if myfile != os.path.join(event.path, event.name):
                    return
                else:
                    line = None
                    # first, check if the file was truncated:
                    curr_size = os.fstat(fh.fileno()).st_size
                    if curr_size < self.last_position_in_file:
                        sys.stdout.write("File was truncated! re-seeking to position 0..\n")
                        self.last_position_in_file = 0
                        fh.seek(0)

                    try:
                        line = fh.readline()
                        self.last_position_in_file = fh.tell()
                    except:
                        sys.stderr.write("error reading line from file!\n")
                        return

                    run(line, k)

            def process_IN_DELETE(self, event):
                return

            def process_IN_CREATE(self, event):
                if myfile in os.path.join(event.path, event.name):
                    # yay, I exist again!
                    global fh
                    fh.close()
                    fh = open(myfile, 'r')
                    # catch up:
                    print "My file was removed, then re-created! I'm now catching up with lines in the newly created file."
                    for line in fh.readlines():
                        run(line, k)
                    fh.seek(0,2)
                    self.last_position_in_file = fh.tell()
                return

        notifier = pyinotify.Notifier(wm, EventHandler())

        # watch the directory, so we can get IN_CREATE events and re-open
        # the file when logrotate comes along.
        index = myfile.rfind('/')
        try:
            wm.add_watch(myfile[:index], dirmask)
        except:
            sys.stderr.write("unable to watch file: %s" % myfile)

        try:
            notifier.loop()
        except KeyboardInterrupt:
            None
        finally:
            notifier.stop()
            fh.close()

