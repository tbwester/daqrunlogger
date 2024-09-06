#!/usr/bin/env python3

from typing import Optional
from datetime import datetime
from collections import deque
import xml.etree.ElementTree as ET

from sbndprmdaq.eclapi import ECL, ECLEntry

from .daqrunlogger import RunInfo


class ECLDAQRunLogger:
    """Posts run info to the E-Log."""

    ECL_CATEGORY = 'DAQ/Automation'
    ECL_START_FORM = 'Run Start'
    ECL_END_FORM = 'Run End'

    def __init__(self, ecl_url, username, password_file='ecl_pwd.txt', min_run=0):
        self._ecl_url = ecl_url
        self._last_posted_time = datetime.fromtimestamp(0)

        # (optional) extra precaution: Don't post if the run is older than this
        self._min_run = min_run

        with open(password_file, 'r') as f:
            password = f.readlines()[0].strip()
        self._ecl_service = ECL(url=self._ecl_url, user=username, password=password)

        self._current_run = None
        self._last_start_ecl_entry_number = None
        self._run_cache = deque(maxlen=1000)


    @staticmethod
    def run_info_from_ecl_entry(entry):
        """
        Create a runinfo object from an ECL XML entry.
        Note the object returned here is used internally for run number and
        time comparisons; we don't try to extract other all fields.
        """
        run_number = ...
        end_time = None
        if entry.attrib['form'] == ECLDAQRunLogger.ECL_START_FORM:
            # get the start time from the current form
            pass
        elif entry.attrib['form'] == ECLDAQRunLogger.ECL_END_FORM:
            # get the end time from the current form
            # try to get the start time from the reference entry
            pass
        else:
            raise ValueError(f'Entry did not correspond to form of type "{ECLDAQRunLogger.ECL_START_FORM}" or "{ECLDAQRunLogger.ECL_END_FORM}".')

        return RunInfo(run_number, start_time=start_time,
                configuration='', metadata='', end_time=end_time)

    def _get_last_run_number(self) -> Optional[RunInfo]:
        """Search the ECL for the last post made by this class. If found, get the
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

        last_run = ECLDAQRunLogger.run_info_from_ecl_entry(last_entry)
        if entry.attrib['formname'] == ECLDAQRunLogger.ECL_START_FORM:
            self._last_start_ecl_entry_number = last_entry.attrib['id'] 
        return self._last_run_number


    def _post_run(self, info: RunInfo, end_of_run: bool=False) -> None:
        """Make the post to the ECL."""
        form_name = ECLDAQRunLogger.ECL_START_FORM if not end_of_run \
            else ECLDAQRunLogger.ECL_END_FORM

        kwargs = {
                'category': ECLDAQRunLogger.ECL_CATEGORY,
                'formname': form_name
        }
        if end_of_run:
            kwargs['related_entry'] = self._last_start_ecl_entry_number

        entry = ECLEntry(**kwargs)

        time_str = info.start_time.strftime('%Y-%m-%d %H:%M:%S')
        if end_of_run:
            time_str = info.end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        fields = {
            'number': str(info.run_number),
        }
        if not end_of_run:
            fields['start_time'] = time_str
            fields['configuration'] = info.configuration
            fields['components'] = ', '.join(info.components)
            fields['metadata'] = info.metadata
        else:
            fields['end_time'] = time_str
            fields['crashed'] = 'Yes' if info.bad_end else 'No'

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

        # first time start up: Check ECL for a previous run
        if self._current_run is None:
            self._current_run = self._get_last_run()

            # if we don't find anything, treat the current run as the first
            # note: only the next run will be posted in this case
            if self._current_run is None:
                self._current_run = info

        self._post_run(info, end_of_run=False)
        return
        if info.run_number == self._current_run.run_number:
            # only post if this is an end-of-run message
            if info.end_time is not None and self._current_run.end_time is None:
                self._post_run(info, end_of_run=True)
                self._run_cache.append(info.run_number)
                self._current_run = info.copy()
        elif info.run_number > self._current_run.run_number:
            # presumably a new run
            if self._current_run.end_time is None:
                # our last post was a start-of-run post, but never got an end time. 
                # Make sure to add an end-of-post message for it, and cache it so
                # we don't process it again
                current_run_end = self._current_run.copy()
                self._post_run(current_run_end, end_of_run=True)
                self._run_cache.append(current_run_end.run_number)
        
            # post start-of-run! Note: don't add to cache until it ends
            self._post_run(info)
            self._current_run = info

            # handle the case where we are given a single completed run
            # that is newer but hasn't been logged at all. We can post the
            # end-of-run entry for it too
            if info.end_time is not None:
                self._post_run(info, end_of_run=True)
                self._run_cache.append(info.run_number)

        else:
            # probably bad input, don't post
            return

