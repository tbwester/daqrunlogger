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
    components: List[str] = field(default_factory=lambda: [])
    end_time: Optional[datetime] = None
    version: Optional[str] = ''
    comments: Optional[str] = ''


class DAQRunLogger(Protocol):
    def log_run(self, info: RunInfo) -> None: ...


class StdoutDAQRunLogger:
    """Prints the RunInfo object to stdout."""
    def __init__(self):
        pass

    def log_run(self, info: RunInfo) -> None:
        print(info)



if __name__ == '__main__':
    s = StdoutDAQRunLogger()
    s.log_run(RunInfo(17215, datetime.now(), 'bnbTest', end_time=datetime.now(), comments='test comment'))
