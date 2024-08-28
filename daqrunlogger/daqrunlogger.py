#!/usr/bin/env python3

"""
Classes to automate logging DAQ runs to different services: ECL & Google sheets
"""

from dataclasses import dataclass, field
from typing import List, Protocol, Optional
from datetime import datetime



@dataclass
class RunInfo:
    run_number: int
    start_time: datetime
    configuration: str
    metadata: str
    components: List[str] = field(default_factory=lambda: [])
    end_time: Optional[datetime] = None
    version: Optional[str] = None
    comments: Optional[str] = None
    bad_end: bool = False
    dev_run: bool = False


class DAQRunLogger(Protocol):
    """Methods for loggers to implement."""
    def log_run(self, info: RunInfo) -> None: ...

    def filter_run(self, info: RunInfo) -> bool: ...


class StdoutDAQRunLogger:
    """Prints the RunInfo object to stdout."""
    def __init__(self):
        pass

    def log_run(self, info: RunInfo) -> None:
        print(info)



if __name__ == '__main__':
    s = StdoutDAQRunLogger()
    s.log_run(RunInfo(17215, datetime.now(), 'bnbTest', end_time=datetime.now(), comments='test comment'))
