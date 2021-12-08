#!/bin/bash

if [[ $1 != "" ]]; then 
    echo "Path in hdfs required"
    exit 1
else
    LATEST_PATH=$1
fi

# delete all but last 7 days of logs
last_week=$(date -d "last week 13:00" '+%s')

# iterate through folders in descending order
for dir in $( hadoop fs -ls -R ${LATEST_PATH} | grep ^d | awk '{print $NF}' | sort -r )
do
    # remove directory if empty
    if [[ $(ls -A ${dir} | wc -l) == 0 ]]; then
        hadoop fs -rmdir ${dir}
        echo "Deleted ${dir}"
    else
        # format path as datelike string YYYY-- YYYY-mm- YYYY-mm-dd
        date_string=$(echo ${dir} | sed -r "s|${LATEST_PATH}([0-9]{4})\/?([0-9]{2})?\/?([0-9]{2})?|\1-\2-\3|")

        # if non-empty monthly directory continue on to next folder
        if [[ ${date_string} =~ [0-9]{1}-$ ]]; then
            continue
        fi

        # get date in seconds from string
        # year is parsed as today's date from that year
        dir_date=$(date -d ${date_string} '+%s')

        # if folder is from before last week remove it
        if [[ ${dir_date} -lt ${last_week} ]]; then
            rm -r ${dir}
            #echo "${dir} and contents removed"
        fi
    fi
done
