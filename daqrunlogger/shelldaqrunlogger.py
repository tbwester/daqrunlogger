#!/usr/bin/env python3

import subprocess
from datetime import datetime, timezone
from typing import Optional, List
from collections import deque

from .daqrunlogger import RunInfo


class ShellDAQRunLogger:
    """Runs a shell command based on the run info. Optionally construct this
    logger with a list of attributes which are passed as arguments to the shell
    command."""

    def __init__(self, shell_cmd: str, forward_attrs: Optional[List[str]]=None, date_format: str='%Y-%m-%d %H:%M:%S'):
        self._shell_cmd = shell_cmd
        self._forward_attrs = []
        if forward_attrs is not None:
            self._forward_attrs = forward_attrs
        self._date_format = date_format
        self._last_return_code = None


    def filter_run(self, info: RunInfo) -> bool:
        """Accept any run."""
        return True


    def log_run(self, info: RunInfo) -> None:
        args = [self._shell_cmd]
        for attr_name in self._forward_attrs:
            attr = getattr(info, attr_name)
            if isinstance(attr, (list, tuple)):
                args += [str(val) for val in attr]
            elif isinstance(attr, datetime):
                args += [d for d in attr.strftime(self._date_format).split()]
            elif attr is None:
                args += ['']
            else:
                args += [str(attr)]

        result = subprocess.run(args)
        self._last_return_code = result.returncode


class OnStartDAQRunLogger(ShellDAQRunLogger):
    """Only accept runs with start times within the last N seconds, then hang
    on to the run until it's processed."""

    def __init__(self, shell_cmd: str, forward_attrs: Optional[List[str]]=None, date_format: str='%Y-%m-%d %H:%M:%S', max_delay: int=60*60*24):
        super().__init__(shell_cmd, forward_attrs, date_format)
        self.max_delay = max_delay
        self.current_run = None
        self.cache = deque(maxlen=1000)

    def filter_run(self, info: RunInfo) -> bool:
        # we've already processed this run
        if info.dev_run:
            # print(f'skip run {info.run_number}, started from dev area')
            return False

        if info.run_number in self.cache:
            # print(f'Skipping known run {info.run_number}')
            return False

        # a completed run. If it's our current one but we haven't processed it
        # yet, process it but also cache it so we don't re-run it
        if info.end_time is not None:
            if self.current_run is not None:
                if info.run_number == self.current_run.run_number and not info.run_number in self.cache:
                    self.cache.append(info.run_number)
                    self.current_run = None
                    return True

            # print(f'Skipping completed run {info.run_number}')
            return False

        # ongoing run & its the current one, let's process it
        if self.current_run is not None:
            if info.run_number < self.current_run.run_number:
                # an old run missing an end time
                return False

            if info.run_number > self.current_run.run_number:
                # Must be a new run & we missed the end time of our current one. Reset
                # for the new one
                self.current_run = info

            return True

        # we don't have a run so this could be the current one
        # let's do a sanity check that it isn't >1 day old
        now = datetime.now(timezone.utc)
        dt = (now - info.start_time).total_seconds()
        if dt > self.max_delay:
            # print(f'Skipping incomplete run {info.run_number}, started more than {self.max_delay} seconds ago.')
            return False
        
        # ok, seems plausible
        self.current_run = info
        return True

    def log_run(self, info: RunInfo) -> None:
        super().log_run(info)
        if self._last_return_code == 0:
            print(f'Shell logger completed ongoing run {info.run_number}')
            self.cache.append(info.run_number)
            self.current_run = None


if __name__ == '__main__':
    s = ShellDAQRunLogger('echo', ['run_number'])
    s.log_run(RunInfo(17215, datetime.now(), 'bnbTest', end_time=datetime.now(), comments='test comment'))
