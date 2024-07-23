#!/usr/bin/env python3

from sbndprmdaq.eclapi import ECL, ECLEntry

from .DAQRunLogger import RunInfo


class ECLDAQRunLogger:
    """Posts run info to the E-Log. Some extra checks ensure we don't make
    multiple posts."""

    def __init__(self, ecl_url):
        self._ecl_url = ecl_url
        self._last_posted_time = datetime.fromtimestamp(0)
        self._ecl_service = ECL(url=self._ecl_url, user='sbnddaq', password='')


    def _get_last_run(self) -> Optional[RunInfo]:
        """Search the ECL for a post made by this class. If any found, get the
        info back as a RunInfo object."""
        return None


    def _post_run(self, info: RunInfo, end_of_run: bool=False) -> None:
        """Make the post to the ECL."""
        date = info.start_time.strftime('%y/%m/%d')
        time = info.start_time.strftime('%H:%M:%S')

        end_time = ''
        if info.end_time is not None:
            end_time = info.end_time.strftime('%H:%M:%S')


    def log_run(self, info: RunInfo) -> None:
        # rate limit to 30 seconds between requests
        now = datetime.now()
        if (now - self._last_posted_time).seconds < 30:
            return

        # some logic depending on the last run posted to the ECL
        # - if we are handling the same run: Only post an end-of-run update if
        #   the current run has an end time and the previous entry did not
        # - if we are handling a new run: Check if the previous run was ended.
        #   If not, post an end-of-run update for that run first
        # - if we are handling an old run, something's wrong, so don't post
        last_run = self._get_last_run() 
        if info.run_number == last_run.run_number:
            if info.end_time is not None and last_run.end_time is None:
                # post an end-of-run message
                self._post_run(info, end_of_run=True)
        elif info.run_number > last_run_number:
            # presumably a new run
            if last_run.end_time is None:
                # our last post was a start-of-run post, make sure to add 
                last_run_end = last_run.copy()
                last_run_end.end_time = info.start_time
                self._post_run(last_run_end, end_of_run=True)
            self._post_run(info)
        else:
            # probably bad input, don't post
            return

        self._last_posted_time = now
