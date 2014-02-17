#!/usr/bin/env sh

# shell 상에서 map reduce를 simulation 하는 방법

cat NASDAQ_daily_prices_A.csv | ./stockrisemap.py | sort | ./stockrisereduce.py
