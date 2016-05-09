# -*- coding: utf-8 -*-
"""
    File:    test_FilesExtractor.py
    Author:  Daniil Dolbilov
    Created: 09-May-2016
"""

import unittest
from FilesExtractor import FilesExtractor

dbnames1 = ['D:/list-files/asus2data-2016.05.07.sqlite', 'D:/list-files/flash4gb-2016.05.07.sqlite']
dbnames2 = ['D:/list-files/asus2data-2016.05.08.sqlite', 'D:/list-files/flash4gb-2016.05.08.sqlite']
rootList = {'asus2data-2016.05.08': u'D:/',
            'flash4gb-2016.05.08': u'F:/'}


class TestFilesExtractorUsage(unittest.TestCase):

    def test_extract_files(self):
        from logging import DEBUG, INFO
        from helpers import initLogs
        initLogs(u"test_FilesExtractor.log", fileAppend=False, fileLevel=DEBUG, consoleLevel=INFO)

        self.extr = FilesExtractor()
        self.extr.compare(dbnames1, dbnames2, True, True)
        self.extr.extractUniqueFiles('table2_unique', rootList, u'./unique2/')


if __name__ == '__main__':
    unittest.main(verbosity=2)
