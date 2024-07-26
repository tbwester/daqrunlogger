#!/usr/bin/env python3

import sys 
import time
from typing import Optional, List
from datetime import datetime

from googleapiclient.discovery import build
from google.oauth2 import service_account

from .daqrunlogger import RunInfo


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
        self._last_post = datetime.fromtimestamp(0)

        credentials = service_account.Credentials.from_service_account_file(
            credentials_filename, scopes=GoogleSheetsDAQRunLogger.SCOPES)
        self._service = build('sheets', 'v4', credentials=credentials)

        # maintain a list of known completed runs so we can skip duplicates
        self._run_cache = []


    def run_row_map(self):
        """Gets valid run numbers from the first column of the spreadsheet. If
        the run appears multiple times, the last row it appears will be
        returned."""
        row_start = str(self._header) if self._header > 0 else ''
        range_name = f'{self._sheet_name}!A{row_start}:A'

        result = self._service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet_id, range=range_name).execute()
        rows = result.get('values', [])
        result = {}

        for i, row in enumerate(rows):
            try:
                result[int(row[0])] = self._header + i + 1
            except (ValueError, IndexError):
                # apply header offset to get correct row
                print(f'Warning: Invalid run number "{row}" at row={self._header + i}.')

        return result


    def log_run(self, info: RunInfo) -> None:
        print(f'got run {info.run_number}', self._run_cache)
        if info.run_number in self._run_cache:
            print(f'skip run {info.run_number}, found in cache')
            return

        # rate limit to 1 seconds between requests
        now = datetime.now()
        dt = (now - self._last_post).total_seconds()
        if dt < 1:
            time.sleep(1 - dt)
        print(f'logging run {info.run_number}')

        date = info.start_time.strftime('%y/%m/%d')
        time_ = info.start_time.strftime('%H:%M:%S')

        end_time = ''
        if info.end_time is not None:
            end_time = info.end_time.strftime('%H:%M:%S')

        body = {
            'values': [
                [
                    info.run_number, date, time_, end_time, info.configuration,\
                    ', '.join(info.excluded_components), info.comments
                ]
            ]
        }

        runs = self.run_row_map()
        row = None
        try:
            row = runs[info.run_number] - 1
            print(f'Found row={row}')
        except KeyError:
            print(f'Found new run, appending')
            pass

        if row is not None:
            result = self._service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id, range=f'A{row}:Z{row}',
                valueInputOption=GoogleSheetsDAQRunLogger.INPUT_OPTS, body=body).execute()
        else:
            result = self._service.spreadsheets().values().append(
                spreadsheetId=self._spreadsheet_id, range=self._range_phrase,
                valueInputOption=GoogleSheetsDAQRunLogger.INPUT_OPTS, body=body).execute()

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
            print('Warning: Unexpected result')
            print(result)

        if info.end_time is not None:
            self._run_cache.append(info.run_number) 
        self._last_post = now
