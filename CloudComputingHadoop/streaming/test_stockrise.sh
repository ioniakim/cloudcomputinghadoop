#!/usr/bin/env sh

cat NASDAQ_daily_prices_A.csv | ./stockrisemap.py | sort | ./stockrisereduce.py
