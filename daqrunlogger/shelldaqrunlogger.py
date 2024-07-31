#!/usr/bin/env python3

import subprocess
from datetime import datetime
from typing import Optional, List

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

        subprocess.run(args)


if __name__ == '__main__':
    s = ShellDAQRunLogger('echo', ['run_number'])
    s.log_run(RunInfo(17215, datetime.now(), 'bnbTest', end_time=datetime.now(), comments='test comment'))
