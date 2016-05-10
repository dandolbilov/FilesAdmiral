# FilesAdmiral

Process SQLite snapshots of file system (created via my FileSystemScan tool). <br />

Feature list: <br />
[+] Compare snapshots and find unique files (two-way comparison is possible). <br />
[+] Can use (combine) multiple snapshots on the left and on the right of compare. <br />
[+] Extract (copy) unique files to target folder (and write single MD5 file for extracted files). <br />

TODO: <br />
[-] GUI (PyQt4) version and console version. <br />
[-] Integrate with FileSystemScan tool (to work with current file system). <br />
[-] Find and process duplicate files. <br />
[-] Mount/connect to virtual file system to work via some File Commander (manual sorting, etc.). <br />
[-] Process archives and other file containers as folders. <br />
[-] Classify files (use auto-filters, add tags manually, grouping files, etc.). <br />
[-] Work with groups (atomic operations, file priority inside group, etc.). <br />
[-] Smart backups between storages. <br />

Usage: <br />
Look at TestFileListComparatorUsage and TestFilesExtractorUsage. <br />
