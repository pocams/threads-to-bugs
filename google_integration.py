import datetime
import logging
from typing import List

import pygsheets
from cachetools.func import ttl_cache

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from data import slugify, Thread

logger = logging.getLogger(__name__)

WORKBOOK_CACHED_FOR = datetime.timedelta(minutes=1)



class WeirdSpreadsheet(Exception):
    pass


def column_letters():
    for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        yield ch

    for a in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        for b in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            yield a + b


def index_to_letter(index):
    letters = column_letters()
    for i in range(index - 1):
        next(letters)
    return next(letters)


# class Headers:
#     def __init__(self, row):
#         self.header_columns = { header: letter for header, letter in zip(row, column_letters()) if header.strip() != "" }
#         self.slugified_columns = { slugify(header): header for header in row if header.strip() != "" }
#
#     def get_column_letter(self, header):
#         spreadsheet_column_title = self.slugified_columns.get(slugify(header))
#         if spreadsheet_column_title:
#             return self.header_columns[spreadsheet_column_title]
#         return None


def get_value_from_cell(cell):
    try:
        return get_value_from_union(cell["userEnteredValue"])
    except KeyError:
        return None


def get_value_from_union(value_union):
    if len(value_union) != 1:
        raise ValueError(f"get_value expected a google sheets style value union, got {value_union!r}")

    [(vtype, value)] = list(value_union.items())

    if vtype == "stringValue":
        return value
    elif vtype == "formulaValue":
        return value
    else:
        raise ValueError(f"Unhandled value type: {value_union!r}")


class GoogleWorkbook:
    def __init__(self, service_file, workbook_key, sheet_title="Discord Sync"):
        self.workbook_key = workbook_key
        self.sheet_title = sheet_title

        credentials = Credentials.from_service_account_file(service_file)
        service = build("sheets", "v4", credentials=credentials)
        self._spreadsheets = service.spreadsheets()

        self._sheet = None
        self._workbook = None
        self._workbook_fetched_at = None
        self._rows_added = 0

    @property
    def workbook(self):
        if self._workbook_fetched_at is None or self._workbook_fetched_at + WORKBOOK_CACHED_FOR < datetime.datetime.now():
            self._store_workbook(self._spreadsheets.get(spreadsheetId=self.workbook_key, includeGridData=True).execute())

        return self._workbook

    def _store_workbook(self, workbook):
        self._workbook = workbook
        self._sheet = None
        self._workbook_fetched_at = datetime.datetime.now()
        self._rows_added = 0

    @property
    def sheet(self):
        if self._sheet is None:
            for sheet in self.workbook["sheets"]:
                if sheet["properties"]["title"].lower() == self.sheet_title.lower():
                    self._sheet = sheet

            if self._sheet is None:
                found_titles = [s["properties"]["title"] for s in self.data["sheets"]]
                raise WeirdSpreadsheet(f"No sheets found with title {self.sheet_title}; found {found_titles}")

        return self._sheet

    @property
    def headers(self):
        row = self.sheet["data"][0]["rowData"][0]
        return [get_value_from_cell(cell) for cell in row["values"]]

    @property
    def discord_id_column_index(self):
        for i, header in enumerate(self.headers):
            if header and slugify(header) == "discord-id":
                return i
        raise WeirdSpreadsheet("No 'Discord ID' column found")

    def do_batch_update(self, requests):
        resp = self._spreadsheets.batchUpdate(
            spreadsheetId=self.workbook_key,
            body={
                "includeSpreadsheetInResponse": True,
                "responseIncludeGridData": True,
                "requests": requests,
            }
        ).execute()
        self._store_workbook(resp["updatedSpreadsheet"])

    def _sync_thread_request(self, thread: Thread):
        row_to_update = None
        for row_index, row in enumerate(self.sheet["data"][0]["rowData"]):
            if row_index == 0:
                continue
            if not row:
                continue

            discord_id = get_value_from_cell(row["values"][self.discord_id_column_index])

            try:
                discord_id = int(discord_id.strip())
            except ValueError:
                pass

            if discord_id == thread.discord_id:
                row_to_update = row_index
                break

        if row_to_update is None:
            row_to_update = len(self.sheet["data"][0]["rowData"]) + self._rows_added
            self._rows_added += 1

        new_row_data = [thread.get_by_header(header).json_data for header in self.headers]

        return {
                "updateCells": {
                    "start": {
                        "sheetId": self.sheet["properties"]["sheetId"],
                        "rowIndex": row_to_update,
                        "columnIndex": 0
                    },
                    "rows": {
                        "values": new_row_data
                    },
                    "fields": "*"
                }
            }

    def sync_thread(self, thread: Thread):
        self.do_batch_update([self._sync_thread_request(thread)])

    def sync_threads(self, threads: List[Thread]):
        self.do_batch_update([self._sync_thread_request(thread) for thread in threads])
