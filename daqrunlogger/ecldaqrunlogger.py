#!/usr/bin/env python3

import time
import dataclasses

from typing import Optional
from datetime import datetime, timezone, timedelta
from collections import deque
import xml.etree.ElementTree as ET

from ecl_api import ECL, ECLEntry

from .daqrunlogger import RunInfo

import logging
logger = logging.getLogger(__name__)


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
        self._run_cache = deque(maxlen=1000)
        self.start_time_utc = datetime.now(tz=timezone.utc)


    @staticmethod
    def run_info_from_ecl_entry(entry):
        """
        Create a runinfo object from an ECL XML entry.
        Note the object returned here is used internally for run number
        comparisons; we don't try to extract other all fields.
        """
        if entry.attrib['form'] not in [ECLDAQRunLogger.ECL_START_FORM, ECLDAQRunLogger.ECL_END_FORM]:
            raise ValueError(f'Entry did not correspond to form of type "{ECLDAQRunLogger.ECL_START_FORM}" or "{ECLDAQRunLogger.ECL_END_FORM}".')

        now = datetime.now(tz=timezone.utc)
        try:
            body = entry.find('./text-html')
            table = ET.fromstring(body.text)
            run_number = int(table.find('./tr/td/pre').text)
        except Exception as e:
            logger.exception(e)
            return None

        return RunInfo(run_number, start_time=now,
                configuration='', metadata='', end_time=None)

    def _get_start_post_for_run(self, run_number) -> Optional[RunInfo]:
        """Return the ECL entry number for a start-of-run post made by this class."""

        response = self._ecl_service.search(category=ECLDAQRunLogger.ECL_CATEGORY, limit=20)
        xml = ET.fromstring(response)
        entries = xml.findall('./entry')
        entries = [e for e in entries if e.attrib['form'] == ECLDAQRunLogger.ECL_START_FORM]

        if not entries:
            return None

        for e in entries:
            info = ECLDAQRunLogger.run_info_from_ecl_entry(e)
            if info is None:
                logger.warn(f'Could not parse entry {e}')
                continue

            if info.run_number == run_number:
                return e.attrib['id']

        return None


    def _post_run(self, info: RunInfo, end_of_run: bool=False) -> None:
        """Make the post to the ECL."""
        logger.info(f'Writing run {info.run_number} to the ECL! {end_of_run=}')

        form_name = ECLDAQRunLogger.ECL_START_FORM if not end_of_run \
            else ECLDAQRunLogger.ECL_END_FORM

        kwargs = {
                'category': ECLDAQRunLogger.ECL_CATEGORY,
                'formname': form_name
        }

        start_ecl_entry_number = self._get_start_post_for_run(info.run_number)
        if end_of_run and start_ecl_entry_number is not None:
            kwargs['related_entry'] = start_ecl_entry_number

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
            fields['included_components'] = '\n'.join(info.components)
            fields['metadata'] = info.metadata
        else:
            fields['end_time'] = time_str
            fields['crashed'] = 'Yes' if info.bad_end else 'No'
            total_seconds = (info.end_time - info.start_time).total_seconds()
            fields['duration'] = str(timedelta(seconds=total_seconds))

        for key, value in fields.items():
            entry.set_value(key, value)

        logger.info(entry.show().strip()[1:])
        self._ecl_service.post(entry, do_post=True)


    def filter_run(self, info: RunInfo) -> bool:
        if info.run_number < self._min_run:
            # an old run, don't handle it
            logger.info(f'skipping run {info.run_number}, too old')
            return False

        if info.dev_run:
            logger.info(f'skip run {info.run_number}, started from dev area')
            return False

        if info.run_number in self._run_cache:
            # we already posted this
            logger.info(f'skipping run {info.run_number}, found in cache')
            return False

        return True


    def log_run(self, info: RunInfo) -> None:
        # rate limit this function to 30 seconds between requests
        now = datetime.now()
        dt = (now - self._last_posted_time).total_seconds()
        if dt < 30:
            time.sleep(30 - dt)
        self._last_posted_time = now
        
        logger.info(f'logging run {info.run_number}')

        # some logic depending on the last run posted to the ECL
        # - if we are handling the same run: Only post an end-of-run update if
        #   the current run has an end time and the previous entry did not
        # - if we are handling a new run: Check if the previous run was ended.
        #   If not, post an end-of-run update for that run first
        # - if we are handling an old run, something's wrong, so don't post

        # first time start up, treat this run as the first
        # note: currently running run does not get a start-of-run entry
        if self._current_run is None:
            logger.info(f'setting run {info.run_number} as the current run')
            self._current_run = info
            return

        if info.run_number < self._current_run.run_number:
            # this is definitely not the latest run, cache it
            self._run_cache.append(info.run_number)
            return

        if info.run_number == self._current_run.run_number:
            # our current run. check if it now has an end time & post the end-of-run entry
            # otherwise, do nothing
            if info.end_time is not None and self._current_run.end_time is None:
                logger.info(f'posting end-of-run for run {info.run_number}')
                self._post_run(info, end_of_run=True)
                self._run_cache.append(info.run_number)
                # this just does a copy; we aren't replacing anything
                self._current_run = dataclasses.replace(info)
            else:
                logger.info(f'waiting on current run {self._current_run.run_number}')
            return

        # presumably a newer run
        if self._current_run.end_time is None:
            # our current run never got an end time. 
            # Make sure to add an end-of-post message for it, and cache it so
            # we don't process it again
            logger.warn(f'posting end-of-run for run {self._current_run.run_number}, but end time was not found!')
            # this just does a copy; we aren't replacing anything
            current_run_end = dataclasses.replace(self._current_run)
            self._post_run(current_run_end, end_of_run=True)
            self._run_cache.append(current_run_end.run_number)
            
        # this run is newer than our current run. Update our current run
        # also cache this so we don't re-process it
        logger.info(f'setting run {info.run_number} as the current run, newer than previous')
        self._current_run = info
        if info.end_time is not None:
            # the run is already completed, so we won't post about it ever
            self._run_cache.append(info.run_number)
            return
        
        # guard against posting a start-of-run entry the first time we start
        # i.e., don't post start-of-run if we didn't actually observe the start time
        if info.start_time < self.start_time_utc:
            logger.info(f'skipping start-of-run post for {info.run_number}, started before us')
            return

        # post start-of-run! Note: don't add to cache until it ends
        logger.info(f'posting start-of-run for run {info.run_number}')
        self._post_run(info)
