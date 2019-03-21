#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Writer(object):

    print('writter ....... 1')
    next_writer_builder = None
    converter_builder = None

    def __init__(
        self,
        next_writer_builder=None,
        converter_builder=None
    ):
        self.next_writer_builder = next_writer_builder
        self.converter_builder = converter_builder
        print('writter ....... 3')

    def opened(self):
        print('writter ....... 4')
        pass

    def open(self):
        print('writter ....... 5')
        pass

    def listify(self, obj_or_list):
        print('writter ....... 6')
        """Ensure list - create a list from an object as [obj] or keep a list if it is already a list"""

        if obj_or_list is None:
            print('writter ....... 7')
            # no value - return empty list
            return []

        elif isinstance(obj_or_list, list) or isinstance(obj_or_list, set) or isinstance(obj_or_list, tuple):
            print('writter ....... 8')
            if len(obj_or_list) < 1:
                print('writter ....... 9')
                # list/set/tuple is empty - nothing to do
                return []
            else:
                print('writter ....... 10')
                # list/set/tuple is good
                return obj_or_list

        else:
            print('writter ....... 11')
            # event_or_events is an object
            return [obj_or_list]

    def convert(self, data):
        print('writter ....... 12')
        """Convert an object if we have a converter or just return object 'as is' otherwise"""
        return self.converter_builder.get().convert(data) if self.converter_builder else data

    def insert(self, event_or_events=None):
        print('writter ....... 13')
        # event_or_events = [
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        #   event: {
        #       row: {'id': 3, 'a': 3}
        #   },
        # ]
        pass

    def flush(self):
        print('writter ....... 14')
        pass

    def push(self):
        print('writter ....... 15')
        pass

    def destroy(self):
        print('writter ....... 16')
        pass

    def close(self):
        print('writter ....... 16')
        pass
