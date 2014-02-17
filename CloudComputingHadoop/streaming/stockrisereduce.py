#!/usr/bin/env python

import sys

last_key = None
rise_sum = 0
for line in sys.stdin:
    key, strvalue = line.strip().split('\t')
    if last_key and last_key != key:
        print '%s\t%s' % (last_key, rise_sum)
        rise_sum = 0
    last_key = key
    value = int(strvalue)
    rise_sum += value

if last_key:
    print '%s\t%s' % (last_key, rise_sum)
