#!/bin/sh

last_week=${date -d "last week 13:00" '+%Y-%m-%d'}

for 


(( $(date -d "2014-12-01T21:34:03+02:00" +%s) < $(date -d "2014-12-01T21:35:03+02:00" +%s) ))

for filename in `hadoop fs -ls /path/to/directory | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`
do echo $filename; 