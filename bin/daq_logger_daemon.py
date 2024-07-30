#!/usr/bin/env python3

import sys
import time
from queue import Queue
import datetime

import daemon

from daqrunlogger import StdoutDAQRunLogger, DAQLoggerWorker


def main():
    # add loggers to the list
    # gslogger = GoogleSheetsDAQRunLogger('sheet id', 'sheet name', header=2)
    stdoutlogger = StdoutDAQRunLogger()
    loggers = [stdoutlogger] #, gslogger]

    queues = []
    workers = []
    for logger in loggers:
        q = Queue()
        w = DAQLoggerWorker(logger, q)
        queues.append(q)
        workers.append(w)

        w.start()

    print(f'Started with {len(workers)} workers.')
    new_run = False
    while True:
        # check the DAQ log for new runs
        if not new_run:
            print('No new run, sleeping...')
            time.sleep(1)
            continue

        # post the run to all the loggers
        for q in queues:
            q.put(info)


if __name__ == '__main__':
    with daemon.DaemonContext(stdout=sys.stdout, stderr=sys.stderr, detach_process=False):
        main()
