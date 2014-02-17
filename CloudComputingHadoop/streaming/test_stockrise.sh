#!/usr/bin/env sh

# shell에서 hadoop map reduce를 simulation 하기 위한 방법

cat NASDAQ_daily_prices_A.csv | ./stockrisemap.py | sort | ./stockrisereduce.py
