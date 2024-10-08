#!/usr/bin/env python3

import sys 
import time
import string
from collections import deque
from typing import Optional, List
from datetime import datetime, timezone

from googleapiclient.http import HttpError
from googleapiclient.discovery import build
from google.oauth2 import service_account

from .daqrunlogger import RunInfo

import logging
logger = logging.getLogger(__name__)


class GoogleSheetsDAQRunLogger:
    """Adds run info to a Google sheet. Assumes column 'A' contains run numbers
    starting at row 1 plus fixed number of header rows."""

    INPUT_OPTS = 'USER_ENTERED'
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
    ]

    def __init__(self, sheet_id: str, sheet_name: str, credentials_filename: str, header: int=0, range_phrase: Optional[str]=None):
        self._spreadsheet_id = sheet_id
        self._sheet_name = sheet_name
        self._header = header

        if range_phrase is None:
            range_phrase = f'A{self._header + 1}:A{self._header + 1}'

        # range_phrase: append method will look for a table starting with this cell
        self._range_phrase = f'{self._sheet_name}!{range_phrase}'

        self._api_wait_seconds = 10
        self._last_post = datetime.fromtimestamp(0)

        credentials = service_account.Credentials.from_service_account_file(
            credentials_filename, scopes=GoogleSheetsDAQRunLogger.SCOPES)
        self._service = build('sheets', 'v4', credentials=credentials)

        # maintain a list of known completed runs so we can skip duplicates
        # deque so that cache never gets too big
        self._run_cache = deque(maxlen=1000)

    def run_row_map(self):
        """Gets valid run numbers from the first column of the spreadsheet. If
        the run appears multiple times, the last row it appears will be
        returned."""
        row_start = str(self._header) if self._header > 0 else ''
        range_name = f'{self._sheet_name}!A{row_start}:A'

        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._spreadsheet_id, range=range_name).execute()
        except (TimeoutError, HttpError):
            return None

        rows = result.get('values', [])
        result = {}

        for i, row in enumerate(rows):
            try:
                result[int(row[0])] = self._header + i + 1
            except (ValueError, IndexError):
                # apply header offset to get correct row
                logger.warn(f'Warning: Invalid run number "{row}" at row={self._header + i}.')

        return result


    def filter_run(self, info: RunInfo) -> bool:
        """Only post shifter runs to the sheet."""
        if info.dev_run:
            logger.info(f'skip run {info.run_number}, started from dev area')
            return False

        if info.run_number in self._run_cache:
            logger.info(f'skip run {info.run_number}, found in cache')
            return False

        return True

    def log_run(self, info: RunInfo) -> None:

        # rate limit to 1 seconds between requests
        now = datetime.now()
        dt = (now - self._last_post).total_seconds()
        if dt < self._api_wait_seconds:
            time.sleep(self._api_wait_seconds - dt)
        logger.info(f'handling run {info.run_number}')

        start_time = info.start_time.strftime('%Y-%m-%d %H:%M:%S')

        runs = self.run_row_map()
        if runs is None:
            logger.warn('Error when accessing Google sheets API, retrying...')
            return
	
        # End time: If properly set, run has concluded. If not, check if there
	# are runs after this run. If so, maybe run was ended un-gracefully
        end_time = ''
        if info.end_time is not None:
            end_time = info.end_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            if any(run > info.run_number for run in runs.keys()):
                end_time = 'unknown'
            else:
                runtime = int((datetime.now(timezone.utc) - info.start_time).total_seconds())
                runtime_min = runtime // 60
                runtime_sec = runtime % 60
                end_time = f'Running ({runtime_min:02d}:{runtime_sec:02d})'
                #end_time = 'Running'

        # note: don't write comments since it may overwrite what the shifter
        # has written
        body = {
            'values': [
                [
                    info.run_number, start_time, end_time, info.configuration,\
                    ', '.join(info.components), 'Yes' if info.bad_end else 'No'
                ]
            ]
        }

        row = None
        try:
            row = runs[info.run_number] - 1
            logger.debug(f'Found row={row}')
        except KeyError:
            logger.debug(f'Found new run, appending')
            pass

        # get the column name of the last column to update, e.g. column 0 is A, etc.
        endcol = string.ascii_uppercase[len(body['values'][0]) - 1]
        try:
            if row is not None:
                result = self._service.spreadsheets().values().update(
                    spreadsheetId=self._spreadsheet_id, range=f'A{row}:{endcol}{row}',
                    valueInputOption=GoogleSheetsDAQRunLogger.INPUT_OPTS, body=body).execute()
            else:
                result = self._service.spreadsheets().values().append(
                    spreadsheetId=self._spreadsheet_id, range=self._range_phrase,
                    valueInputOption=GoogleSheetsDAQRunLogger.INPUT_OPTS, body=body).execute()
        except (TimeoutError, HttpError):
            logger.warn('Error when accessing Google sheets API, retrying...')
            return

        updated_cells = 0

        # update and append results have different structure
        try:
            updated_cells = result['updates']['updatedCells']
        except KeyError:
            try:
                updated_cells = result['updatedCells']
            except KeyError:
                pass

        if updated_cells == 0:
            logger.warn(f'Warning: Unexpected result {result}')

        if info.end_time is not None:
            self._run_cache.append(info.run_number) 
        self._last_post = now
