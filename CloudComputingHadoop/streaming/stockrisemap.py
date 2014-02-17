#!/usr/bin/env python

import re
import sys

value = 1

for linenum, line in enumerate(sys.stdin):
    if linenum > 0:
        columns = line.strip().split(',')
        if columns != None and len(columns) > 0:
            try:
                year = columns[2][:4]
                rise = float(columns[6]) - float(columns[3])
                if rise > 0:
                    print '%s\t%s' % (year, value)
            except:
                pass
