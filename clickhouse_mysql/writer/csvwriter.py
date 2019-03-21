#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import os.path
import logging
import copy
import time
import uuid

from clickhouse_mysql.writer.writer import Writer
from clickhouse_mysql.event.event import Event

print('csv writter ....... 1')
class CSVWriter(Writer):
    """Write CSV files"""
    print('csv writter ....... 2')
    file = None
    path = None
    writer = None
    dst_schema = None
    dst_table = None
    fieldnames = None
    header_written = False
    path_prefix = None
    path_suffix_parts = []
    delete = False

    def __init__(
            self,
            csv_file_path=None,
            csv_file_path_prefix=None,
            csv_file_path_suffix_parts=[],
            csv_keep_file=False,
            dst_schema=None,
            dst_table=None,
            next_writer_builder=None,
            converter_builder=None,
    ):
        print('csv writter ....... 3')
        logging.info("CSVWriter() "
            "csv_file_path={} "
            "csv_file_path_prefix={} "
            "csv_file_path_suffix_parts={} "
            "csv_keep_file={} "
            "dst_schema={} "
            "dst_table={} ".format(
            csv_file_path,
            csv_file_path_prefix,
            csv_file_path_suffix_parts,
            csv_keep_file,
            dst_schema,
            dst_table,
        ))
        print('csv writter ....... 4')
        super().__init__(next_writer_builder=next_writer_builder, converter_builder=converter_builder)

        self.path = csv_file_path
        self.path_prefix = csv_file_path_prefix
        self.path_suffix_parts = csv_file_path_suffix_parts
        self.dst_schema = dst_schema
        self.dst_table = dst_table
        print('csv writter ....... 5')
        if self.path is None:
            print('csv writter ....... 6')
            if not self.path_suffix_parts:
                print('csv writter ....... 7')
                # no suffix parts specified - use default ones
                # 1. current UNIX timestamp with fractions Ex.: 1521813908.1152523
                # 2. random-generated UUID Ex.: f42d7297-9d25-43d8-8468-a59810ce9f77
                # result would be filename like csvpool_1521813908.1152523_f42d7297-9d25-43d8-8468-a59810ce9f77.csv
                self.path_suffix_parts.append(str(time.time()))
                self.path_suffix_parts.append(str(uuid.uuid4()))
            self.path = self.path_prefix + '_'.join(self.path_suffix_parts) + '.csv'
            self.delete = not csv_keep_file

        logging.info("CSVWriter() self.path={}".format(self.path))

    def __del__(self):
        print('csv writter ....... 8')
        self.destroy()

    def opened(self):
        print('csv writter ....... 9')
        return bool(self.file)

    def open(self):
        print('csv writter ....... 10')
        if not self.opened():
            print('csv writter ....... 11')
            # do not write header to already existing file
            # assume it was written earlier
            if os.path.isfile(self.path):
                self.header_written = True
            # open file for write-at-the-end mode
            self.file = open(self.path, 'a+')

    def insert(self, event_or_events):
        print('csv writter ....... 12')
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        events = self.listify(event_or_events)
        if len(events) < 1:
            print('csv writter ....... 13')
            logging.warning('No events to insert. class: %s', __class__)
            return

        # assume we have at least one Event

        logging.debug('class:%s insert %d events', __class__, len(events))

        if not self.opened():
            print('csv writter ....... 14')
            self.open()

        if not self.writer:
            print('csv writter ....... 15')
            # pick any event from the list
            event = events[0]
            if not event.verify:
                print('csv writter ....... 16')
                logging.warning('Event verification failed. Skip insert(). Event: %s Class: %s', event.meta(), __class__)
                return

            self.fieldnames = sorted(self.convert(copy.copy(event.first_row())).keys())
            if self.dst_schema is None:
                print('csv writter ....... 17')
                self.dst_schema = event.schema
            if self.dst_table is None:
                print('csv writter ....... 18')
                self.dst_table = event.table

            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
            if not self.header_written:
                self.writer.writeheader()

        for event in events:
            print('csv writter ....... 19')
            if not event.verify:
                logging.warning('Event verification failed. Skip one event. Event: %s Class: %s', event.meta(), __class__)
                continue # for event
            for row in event:
                print('csv writter ....... 20')
                self.writer.writerow(self.convert(row))

    def push(self):
        print('csv writter ....... 21')
        if not self.next_writer_builder or not self.fieldnames:
            return

        event = Event()
        event.schema = self.dst_schema
        event.table = self.dst_table
        event.filename = self.path
        event.fieldnames = self.fieldnames
        print('csv writter ....... 22')
        self.next_writer_builder.get().insert(event)

    def close(self):
        print('csv writter ....... 23')
        if self.opened():
            self.file.flush()
            self.file.close()
            self.file = None
            self.writer = None

    def destroy(self):
        print('csv writter ....... 24')
        if self.delete and os.path.isfile(self.path):
            self.close()
            os.remove(self.path)

if __name__ == '__main__':
    print('csv writter ....... 25')
    path = 'file.csv'

    writer = CSVWriter(path)
    writer.open()
    event = Event()
    event.row_converted={
        'a': 123,
        'b': 456,
        'c': 'qwe',
        'd': 'rty',
    }
    writer.insert(event)
    event.row_converted={
        'a': 789,
        'b': 987,
        'c': 'asd',
        'd': 'fgh',
    }
    writer.insert(event)
    writer.close()
