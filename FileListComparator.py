# -*- coding: utf-8 -*-
"""
    File:    FileListComparator.py
    Author:  Daniil Dolbilov
    Created: 08-May-2016
"""

import logging
from pydal import DAL, Field


class FileListComparator(object):
    def __init__(self):
        self.tempdb = None

    def trace(self, event, msg):
        logging.info('%s, %s' % (event, msg))

    def prepareTempDatabase(self, tdbname):
        self.trace('prepare-temp-db-start', tdbname)
        if tdbname == 'sqlite:memory:':
            self.tempdb = DAL(tdbname)
            for tableName in ['table1', 'table2']:
                self.tempdb.define_table(tableName,
                                         Field('fname', type='string', length=256),
                                         Field('fsize', type='integer'),
                                         Field('md5_hash', type='string', length=32),
                                         Field('wtime', type='text'),  # TODO(dan.dolbilov): switch to datetime
                                         Field('sz_md5_name', type='string')
                                         )
        else:
            self.trace('error', 'ERROR: tempdb <> memory is not implemented')
        self.trace('prepare-temp-db-done', tdbname)
        return self.tempdb

    def prepareSnapshotTableFromImage2011(self, tableName, dbname):
        # count rows in dest temp table
        n = self.tempdb(self.tempdb[tableName].id > 0).count()
        self.trace('prepare-table-start', 'table=[%s], count1 = %u, src=[%s]' % (tableName, n, dbname))

        # connect to source database
        db1 = DAL(dbname, migrate_enabled=False)
        db1.define_table('Files',
                         Field('fileId', type='integer'),
                         Field('foId', type='integer'),
                         Field('fname', type='text'),
                         Field('fsize', type='integer'),
                         Field('ctime', type='text'),
                         Field('wtime', type='text'),
                         primarykey=['fileId']
                         )
        db1.define_table('FilesMD5',
                         Field('fileId', type='integer'),
                         Field('md5', type='text'),
                         Field('calcTime', type='text'),
                         primarykey=['fileId']
                         )
        files_with_md5 = db1(db1.Files.fileId == db1.FilesMD5.fileId)

        # copy data from source database to dest temp table
        for x in files_with_md5.select():
            self.tempdb[tableName].insert(
                fname=x.Files.fname, fsize=x.Files.fsize, md5_hash=x.FilesMD5.md5,
                wtime=x.Files.wtime,  # TODO(dan.dolbilov): switch to datetime
                sz_md5_name='%i_%s_%s' % (x.Files.fsize, x.FilesMD5.md5, x.Files.fname)
            )
        self.tempdb.commit()

        # count rows in dest temp table
        n = self.tempdb(self.tempdb[tableName].id > 0).count()
        self.trace('prepare-table-done', 'table=[%s], count2 = %u' % (tableName, n))

    def compare(self, dbnames1, dbnames2, t1unique, t2unique):
        self.trace('compare-start', '')
        self.prepareTempDatabase('sqlite:memory:')

        if 0:  # clear dest temp tables
            self.tempdb(self.tempdb['table1'].id > 0).delete()
            self.tempdb(self.tempdb['table2'].id > 0).delete()

        for dbname1 in dbnames1:
            self.prepareSnapshotTableFromImage2011('table1', 'sqlite://' + dbname1)
        for dbname2 in dbnames2:
            self.prepareSnapshotTableFromImage2011('table2', 'sqlite://' + dbname2)
        tdb = self.tempdb

        if t1unique:
            q2 = tdb()._select(tdb.table2.sz_md5_name)
            for x in tdb(~tdb.table1.sz_md5_name.belongs(q2)).select():
                self.trace('table1-unique',
                           'fname = [%s], fsize = %i, md5 = %s, wtime = [%s]' % (x.fname, x.fsize, x.md5_hash, x.wtime))

        if t2unique:
            q1 = tdb()._select(tdb.table1.sz_md5_name)
            for x in tdb(~tdb.table2.sz_md5_name.belongs(q1)).select():
                self.trace('table2-unique',
                           'fname = [%s], fsize = %i, md5 = %s, wtime = [%s]' % (x.fname, x.fsize, x.md5_hash, x.wtime))

        self.trace('compare-done', '')
