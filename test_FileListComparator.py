# -*- coding: utf-8 -*-
"""
    File:    test_FileListComparator.py
    Author:  Daniil Dolbilov
    Created: 08-May-2016
"""

from __future__ import print_function

import unittest
import timeit
from collections import Counter
from FileListComparator import FileListComparator

dbnames1 = ['D:/list-files/asus2data-2016.05.07.sqlite', 'D:/list-files/flash4gb-2016.05.07.sqlite']
dbnames2 = ['D:/list-files/asus2data-2016.05.08.sqlite', 'D:/list-files/flash4gb-2016.05.08.sqlite']


class TestFileListComparatorUsage(unittest.TestCase):

    def test_compare_with_output(self):
        from logging import DEBUG, INFO
        from helpers import initLogs
        initLogs(u"test_FileListComparator.log", fileAppend=False, fileLevel=DEBUG, consoleLevel=INFO)
        self.comp = FileListComparator()
        self.comp.compare(dbnames1, dbnames2, True, True)


class TestFileListCompare(unittest.TestCase):

    def fake_trace(self, event, msg):
        self.events.append(event)

    def setUp(self):
        self.events = []
        self.comp = FileListComparator()
        self.comp.trace = self.fake_trace

    def test_compare(self):
        self.comp.compare(dbnames1, dbnames2, True, True)
        counter = Counter(self.events)
        self.assertEqual(counter['compare-start'], 1)
        self.assertEqual(counter['compare-done'], 1)
        self.assertEqual(counter['prepare-temp-db-start'], 1)
        self.assertEqual(counter['prepare-temp-db-done'], 1)
        self.assertEqual(counter['prepare-table-start'], 4)
        self.assertEqual(counter['prepare-table-done'], 4)
        self.assertEqual(counter['table1-unique'], 6)
        self.assertEqual(counter['table2-unique'], 9)


class TestFileListJoinSpeed(unittest.TestCase):

    def fake_trace(self, event, msg):
        self.events.append(event)
        if event == 'elapsed':
            self.elapsed = msg

    def setUp(self):
        self.events = []
        self.comp = FileListComparator()
        self.comp.trace = self.fake_trace
        self.comp.prepareTempDatabase('sqlite:memory:')
        self.comp.prepareSnapshotTableFromImage2011('table1', 'sqlite://' + dbnames1[0])
        self.comp.prepareSnapshotTableFromImage2011('table2', 'sqlite://' + dbnames2[0])

    def time_usage(self, func):
        def wrapper(*args, **kwargs):
            start_time = timeit.default_timer()
            func(*args, **kwargs)
            elapsed = timeit.default_timer() - start_time
            comp = args[0]
            comp.trace('elapsed', '%.0f ms' % (elapsed * 1000))

        return wrapper

    def test_sql_join_speed_v1(self):
        @self.time_usage
        def exec_sql_join_v1(comp):  # join v1 - SLOW (9 minutes)
            tdb = self.comp.tempdb
            for x in tdb(tdb.table2.fname == None).select(
                    tdb.table1.ALL,
                    tdb.table2.ALL,
                    left=tdb.table2.on(tdb.table1.sz_md5_name == tdb.table2.sz_md5_name)):
                comp.trace('table1-unique',
                           'fname = [%s], fsize = %i, md5 = %s' % (x.table1.fname, x.table1.fsize, x.table1.md5_hash))

        exec_sql_join_v1(self.comp)

        counter = Counter(self.events)
        self.assertEqual(counter['prepare-temp-db-start'], 1)
        self.assertEqual(counter['prepare-temp-db-done'], 1)
        self.assertEqual(counter['prepare-table-start'], 2)
        self.assertEqual(counter['prepare-table-done'], 2)
        self.assertEqual(counter['table1-unique'], 6)
        self.assertEqual(counter['elapsed'], 1)

    def test_sql_join_speed_v2(self):
        @self.time_usage
        def exec_sql_join_v2(comp):  # join v2 - SLOW (9 minutes)
            tdb = self.comp.tempdb
            common_files = tdb(tdb.table1.sz_md5_name == tdb.table2.sz_md5_name)
            common_list = [(lambda k: k['sz_md5_name'])(x) for x in
                           common_files.select(tdb.table1.sz_md5_name, distinct=True).as_list()]
            for x in tdb(tdb.table1.sz_md5_name).select():
                if x.sz_md5_name not in common_list:
                    comp.trace('table1-unique', 'fname = [%s], fsize = %i, md5 = %s' % (x.fname, x.fsize, x.md5_hash))

        exec_sql_join_v2(self.comp)

        counter = Counter(self.events)
        self.assertEqual(counter['prepare-temp-db-start'], 1)
        self.assertEqual(counter['prepare-temp-db-done'], 1)
        self.assertEqual(counter['prepare-table-start'], 2)
        self.assertEqual(counter['prepare-table-done'], 2)
        self.assertEqual(counter['table1-unique'], 6)
        self.assertEqual(counter['elapsed'], 1)

    def test_sql_join_speed_v3(self):
        @self.time_usage
        def exec_sql_join_v3(comp):  # join v3 - GOOD (1 minute)
            tdb = self.comp.tempdb
            q2 = tdb()._select(tdb.table2.sz_md5_name)
            for x in tdb(~tdb.table1.sz_md5_name.belongs(q2)).select():
                comp.trace('table1-unique', 'fname = [%s], fsize = %i, md5 = %s' % (x.fname, x.fsize, x.md5_hash))

        exec_sql_join_v3(self.comp)

        counter = Counter(self.events)
        self.assertEqual(counter['prepare-temp-db-start'], 1)
        self.assertEqual(counter['prepare-temp-db-done'], 1)
        self.assertEqual(counter['prepare-table-start'], 2)
        self.assertEqual(counter['prepare-table-done'], 2)
        self.assertEqual(counter['table1-unique'], 6)
        self.assertEqual(counter['elapsed'], 1)

    def tearDown(self):
        print('(elapsed = %s) ' % self.elapsed, end='')


if __name__ == '__main__':
    unittest.main(verbosity=2)
