#!/usr/bin/env python3

from sbndprmdaq.eclapi import ECL, ECLEntry

from .DAQRunLogger import RunInfo


class ECLDAQRunLogger:
    """Adds run info to the E-Log."""

    def __init__(self, ecl_url):
        self._ecl_url = ecl_url
        self._last_post = datetime.fromtimestamp(0)


    def log_run(self, info: RunInfo) -> None:
        # rate limit to 30 seconds between requests
        now = datetime.now()
        if (now - self._last_post).seconds < 30:
            return


        date = info.start_time.strftime('%y/%m/%d')
        time = info.start_time.strftime('%H:%M:%S')

        end_time = ''
        if info.end_time is not None:
            end_time = info.end_time.strftime('%H:%M:%S')

