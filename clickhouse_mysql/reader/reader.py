#!/usr/bin/env python
# -*- coding: utf-8 -*-

from clickhouse_mysql.observable import Observable
print('reader ....... 1')

class Reader(Observable):
    print('reader ....... 2')
    """Read data from source and notify observers"""

    converter = None

    event_handlers = {
        # called on each WriteRowsEvent
        'WriteRowsEvent': [],

        # called on each row inside WriteRowsEvent (thus can be called multiple times per WriteRowsEvent)
        'WriteRowsEvent.EachRow': [],

        # called when Reader has no data to read
        'ReaderIdleEvent': [],
    }
    print('reader ....... 3')

    def __init__(self, converter=None, callbacks={}):
        print('reader ....... 4')
        self.converter = converter
        self.subscribe(callbacks)

    def read(self):
        print('reader ....... 5')
        pass
