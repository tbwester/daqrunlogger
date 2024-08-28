#!/usr/bin/env python3

import time
from queue import Queue
from threading import Thread
from datetime import datetime

from .daqrunlogger import RunInfo, DAQRunLogger



class DAQLoggerWorker(Thread):
    """Class to be assigned to a thread. Listens for RunInfo objects and calls
    its DAQRunLogger's log_run method when available."""
    def __init__(self, logger: DAQRunLogger, queue: Queue, timeout: int=1):
        super().__init__()
        self._logger = logger
        self._queue = queue
        # in seconds
        self._timeout = timeout
        self._stopped = False

        # this thread will exit once the main thread does
        self.daemon = True

    
    def stop(self):
        self._stopped = True

    def run(self):
        while not self._stopped:
            if self._queue.empty():
                time.sleep(self._timeout)
                continue

            info = self._queue.get()
            if not self._logger.filter_run(info):
                self._queue.task_done()
                continue

            self._logger.log_run(info)
            self._queue.task_done()


if __name__ == '__main__':
    from DAQRunLogger import StdoutDAQRunLogger
    info_queue = Queue()
    logger = StdoutDAQRunLogger()
    worker = LoggerWorker(logger, info_queue)
    worker.start()

    info = RunInfo(17215, datetime.now(), 'bnbTest', end_time=datetime.now(), comments='test comment')
    for i in range(5):
        info_queue.put(info)
        time.sleep(1)

    while not info_queue.empty():
        sleep(1)
    
    worker.stop()
