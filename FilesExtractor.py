# -*- coding: utf-8 -*-
"""
    File:    FilesExtractor.py
    Author:  Daniil Dolbilov
    Created: 09-May-2016
"""

import os
import shutil
from datetime import datetime
from FileListComparator import FileListComparator
from helpers import normpathEx, calcFileMD5


class FilesExtractor(FileListComparator):

    def __init__(self):
        super(FilesExtractor, self).__init__()

    def extractUniqueFiles(self, tableName, rootList, destPath):
        self.trace('extract-files-start', '[%s] to [%s]' % (tableName, destPath))
        tdb = self.tempdb

        if not os.path.exists(destPath):
            os.makedirs(destPath)
        md5_file = 'unique-%date%.md5'.replace('%date%', datetime.now().strftime("%Y.%m.%d"))
        md5_file = normpathEx(destPath + '/') + md5_file
        md5_head = '; FilesExtractor\n'
        with open(md5_file, "wb") as f:
            f.write(md5_head.encode('windows-1251'))  # TODO(dan.dolbilov): fix MD5 file encoding

        for x in tdb().select(tdb[tableName].ALL):
            a_dk_name = x.dk_name.decode('utf-8')
            a_fo_path = x.fo_path.decode('utf-8')
            a_fname = x.fname.decode('utf-8')

            dir1 = normpathEx(rootList[a_dk_name] + '/' + a_fo_path + '/')
            dir2 = normpathEx(destPath + '/' + a_dk_name + '/' + a_fo_path + '/')
            if not os.path.exists(dir2):
                os.makedirs(dir2)
            srcFile = dir1 + a_fname
            dstFile = dir2 + a_fname

            md5_line = x.md5_hash + ' ' + normpathEx(a_dk_name + '/' + a_fo_path + '/') + a_fname + '\n'
            with open(md5_file, "ab") as f:
                f.write(md5_line.encode('windows-1251'))  # TODO(dan.dolbilov): fix MD5 file encoding

            if x.fsize > 100 * 1024 * 1024:
                self.trace('error', 'ERROR: file copy skipped (fsize = %i), [%s] => [%s]' % (x.fsize, srcFile, dstFile))
                continue

            try:
                shutil.copy2(srcFile, dstFile)
            except IOError:
                self.trace('error', 'ERROR: file copy failed (IOError), [%s] => [%s]' % (srcFile, dstFile))
                continue
            if calcFileMD5(dstFile) != x.md5_hash:
                self.trace('error', 'ERROR: file copy failed (md5 mismatch), [%s] => [%s]' % (srcFile, dstFile))
                continue

            self.trace('file-copy', 'file copy OK, [%s] => [%s]' % (srcFile, dstFile))

        self.trace('extract-files-done', '[%s] to [%s]' % (tableName, destPath))
