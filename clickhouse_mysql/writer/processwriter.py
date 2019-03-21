#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing as mp
import logging

from clickhouse_mysql.writer.writer import Writer

print('process writter ....... 1')
class ProcessWriter(Writer):
    print('process writter ....... 2')
    """Start write procedure as a separated process"""
    args = None

    def __init__(self, **kwargs):
        print('process writter ....... 3')
        next_writer_builder = kwargs.pop('next_writer_builder', None)
        converter_builder = kwargs.pop('converter_builder', None)
        super().__init__(next_writer_builder=next_writer_builder, converter_builder=converter_builder)
        for arg in kwargs:
            print('process writter ....... 4')
            self.next_writer_builder.param(arg, kwargs[arg])

    def opened(self):
        print('process writter ....... 5')
        pass

    def open(self):
        print('process writter ....... 6')
        pass

    def process(self, event_or_events=None):
        print('process writter ....... 7')
        """Separate process body to be run"""

        logging.debug('class:%s process()', __class__)
        writer = self.next_writer_builder.get()
        writer.insert(event_or_events)
        writer.close()
        writer.push()
        writer.destroy()
        logging.debug('class:%s process() done', __class__)

    def insert(self, event_or_events=None):
        print('process writter ....... 8')
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]

        # start separated process with event_or_events to be inserted

        logging.debug('class:%s insert', __class__)
        process = mp.Process(target=self.process, args=(event_or_events,))

        logging.debug('class:%s insert.process.start()', __class__)
        process.start()

        #process.join()
        logging.debug('class:%s insert done', __class__)
        pass

    def flush(self):
        print('process writter ....... 9')
        pass

    def push(self):
        print('process writter ....... 10')
        pass

    def destroy(self):
        print('process writter ....... 11')
        pass

    def close(self):
        print('process writter ....... 12')
        pass
