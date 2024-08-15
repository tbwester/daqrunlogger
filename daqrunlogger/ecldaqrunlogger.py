#!/usr/bin/env python3

from typing import Optional
from datetime import datetime
from collections import deque
import xml.etree.ElementTree as ET

from sbndprmdaq.eclapi import ECL, ECLEntry

from .daqrunlogger import RunInfo


class ECLDAQRunLogger:
    """Posts run info to the E-Log. This class checks to see that the run
    number is greater than the previous to avoid making multiple or
    out-of-order posts, so it is up to the caller to ensure the runs to log are
    sorted."""

    ECL_CATEGORY = 'DAQ/Automation'
    ECL_START_FORM = 'Run Start'
    ECL_END_FORM = 'Run End'

    def __init__(self, ecl_url, username, password_file='ecl_pwd.txt', min_run=0):
        self._ecl_url = ecl_url
        self._last_posted_time = datetime.fromtimestamp(0)

        # extra precaution: Don't post if the run is older than this
        self._min_run = min_run

        with open(password_file, 'r') as f:
            password = f.readlines()[0].strip()
        self._ecl_service = ECL(url=self._ecl_url, user=username, password=password)

        self._last_run_number = None
        self._last_ecl_entry_number = None
        self._run_cache = deque(maxlen=1000)


    def _get_last_run_number(self) -> Optional[RunInfo]:
        """Search the ECL for a post made by this class. If any found, get the
        info back as a RunInfo object."""
        # response = self._ecl_service.search(category=ECLDAQLogger.ECL_CATEGORY, limit=20)
        response = self._ecl_service.search(limit=20)
        xml = ET.fromstring(response)
        entries = xml.findall('./entry')
        # entries = [e for e in entries if e.attrib['form'] in \
        #     [ECLDAQRunLogger.ECL_START_FORM, ECLDAQRunLogger.ECL_END_FORM]]
        if not entries:
            return None

        last_entry = next(reversed(sorted(entries, \
            key=lambda x: datetime.strptime(x.attrib['timestamp'], '%m/%d/%Y %H:%M:%S'))))

        self._last_run_number = ...
        self._last_ecl_entry_number = last_entry.attrib['id'] 
        return self._last_run_number


    def _post_run(self, info: RunInfo, end_of_run: bool=False) -> None:
        """Make the post to the ECL."""
        time_str = info.start_time.strftime('%Y-%m-%d %H:%M:%S')
        if end_of_run:
            time_str = info.end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        fields = {
            'number': str(info.run_number),
            'configuration': info.configuration,
            'components': ', '.join(info.components), 
            'metadata': info.metadata,
        }
        if not end_of_run:
            fields['start_time'] = time_str,
        else:
            fields['end_time'] = time_str,
            fields['crashed'] = 'Yes' if info.bad_end else 'No'

        form_name = ECLDAQRunLogger.ECL_START_FORM if not end_of_run \
            else ECLDAQRunLogger.ECL_END_FORM
        
        entry = ECLEntry(category=ECLDAQRunLogger.ECL_CATEGORY,
            formname=form_name)
        for key, value in fields.items():
            entry.set_value(key, value)

        print(entry.show())


    def log_run(self, info: RunInfo) -> None:
        # rate limit this function to 30 seconds between requests
        now = datetime.now()
        if (now - self._last_posted_time).seconds < 30:
            return

        if info.run_number < self._min_run:
            # an old run, don't handle it
            return

        if info.run_number in self._run_cache:
            # we already posted this
            return
        
        self._last_posted_time = now

        # some logic depending on the last run posted to the ECL
        # - if we are handling the same run: Only post an end-of-run update if
        #   the current run has an end time and the previous entry did not
        # - if we are handling a new run: Check if the previous run was ended.
        #   If not, post an end-of-run update for that run first
        # - if we are handling an old run, something's wrong, so don't post
        last_run_number = self._get_last_run_number() 
        self._post_run(info, end_of_run=False)
        return
        if info.run_number == last_run_number:
            # only post if this is an end-of-run message
            if info.end_time is not None and last_run.end_time is None:
                self._post_run(info, end_of_run=True)
                self._run_cache.append(info.run_number)
        elif info.run_number > last_run_number:
            # presumably a new run
            if last_run.end_time is None:
                # our last post was a start-of-run post, but never got an end time. 
                # Make sure to add an end-of-post message for it
                last_run_end = last_run.copy()
                last_run_end.bad_end = True
                self._post_run(last_run_end, end_of_run=True)
                self._run_cache.append(last_run_end.run_number)
        
            # post it! Note: don't add to cache until it ends
            self._post_run(info)

            # handle the case where we are given a single completed run
            # that is newer but hasn't been logged at all. We can post the
            # end-of-run entry for it too
            if info.end_time is not None:
                self._post_run(info, end_of_run=True)
                self._run_cache.append(info.run_number)

        else:
            # probably bad input, don't post
            return

