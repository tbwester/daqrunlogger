#!/usr/bin/env python3

import sys
import time
from queue import Queue
from datetime import datetime

import daemon

from daqrunlogger import StdoutDAQRunLogger, ShellDAQRunLogger, \
    DAQLoggerWorker, RunInfo


def main():
    # add loggers to the list
    # gslogger = GoogleSheetsDAQRunLogger('sheet id', 'sheet name', header=2)
    stdoutlogger = StdoutDAQRunLogger()
    shelllogger = ShellDAQRunLogger('echo', ['run_number', 'start_time', 'doesn\'t exist'])
    loggers = [stdoutlogger, shelllogger] #, gslogger]

    queues = []
    workers = []
    for logger in loggers:
        q = Queue()
        w = DAQLoggerWorker(logger, q)
        queues.append(q)
        workers.append(w)

        w.start()

    print(f'Started with {len(workers)} workers.')
    new_run = True
    while True:
        # check the DAQ log for new runs
        if not new_run:
            print('No new run, sleeping...')
            time.sleep(1)
            continue

        # post the run to all the loggers
        for q in queues:
            q.put(RunInfo(17215, datetime.now(), 'bnbTest', end_time=datetime.now(), comments='test comment'))
        new_run = False


if __name__ == '__main__':
    with daemon.DaemonContext(stdout=sys.stdout, stderr=sys.stderr, detach_process=False):
        main()
