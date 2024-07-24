#!/usr/bin/env python3

import time
from queue import Queue

import daemon

from daqrunlogger import GoogleSheetsDAQRunLogger, LoggerWorker


def main():
    # add loggers to the list
    # gslogger = GoogleSheetsDAQRunLogger('sheet id', 'sheet name', header=2)
    stdoutlogger = StdoutDAQRunLogger()
    loggers = [stdoutlogger] #, gslogger]

    queues = []
    workers = []
    for logger in loggers:
        q = Queue()
        w = LoggerWorker(logger, q)
        queues.append(q)
        workers.append(w)

        worker.start()

    while True:
        # check the DAQ log for new runs
        if not new_run:
            time.sleep(1)
            continue

        # post the run to all the loggers
        for q in queues:
            q.put(info)


if __name__ == '__main__':
    with daemon.DaemonContext():
        main()
