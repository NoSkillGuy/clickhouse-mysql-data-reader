#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from clickhouse_mysql.writer.writer import Writer
from clickhouse_mysql.event.event import Event
from clickhouse_mysql.pool.bbpool import BBPool
print('pool writter ....... 1')

class PoolWriter(Writer):
    print('pool writter ....... 2')
    """Write with caching in Pool"""

    writer_builder = None
    max_pool_size = None
    pool = None

    def __init__(
            self,
            writer_builder=None,
            max_pool_size=10000,
            max_flush_interval=60
    ):
        logging.info("PoolWriter()")
        print('pool writter ....... 3')
        self.writer_builder = writer_builder
        self.max_pool_size = max_pool_size
        self.max_flush_interval = max_flush_interval
        print('pool writter ....... 4')
        self.pool = BBPool(
            writer_builder=self.writer_builder,
            max_bucket_size=self.max_pool_size,
            max_interval_between_rotations=self.max_flush_interval,
        )
        print('pool writter ....... 5')

    def insert(self, event_or_events):
        print('pool writter ....... 6')
        """Insert data into Pool"""
        logging.debug('class:%s insert', __class__)
        self.pool.insert(event_or_events)

    def flush(self):
        print('pool writter ....... 7')
        self.pool.flush()

if __name__ == '__main__':
    print('pool writter ....... 8')
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
