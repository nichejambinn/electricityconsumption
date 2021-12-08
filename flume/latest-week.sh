#!/bin/bash

if [[ $1 == "" ]]; then 
    LATEST_PATH="/test"
    hadoop fs -rm -r /test
    touch {a..d}.log
    hadoop fs -mkdir -p /test/2015/04/17 /test/2016/09/2{7..8} /test/2021/12/0{5..6} /test/2019/11/02
    hadoop fs -copyFromLocal a.log /test/2016/09/28
    hadoop fs -copyFromLocal b.log /test/2021/12/06
    hadoop fs -copyFromLocal c.log /test/2019/11/02
    hadoop fs -copyFromLocal d.log /test/2021/12/05
    rm {a..d}.log
    echo "Created /test in hdfs"
else
    LATEST_PATH=$1
fi

# delete all but last 7 days of logs
last_week=$(date -d "last week 13:00" '+%s')
past_week=0

# iterate through folders in descending order
for dir in $( hadoop fs -ls -R ${LATEST_PATH} | grep ^d | awk '{print $NF}' | sort -r )
do
    # directories are sorted from most recent on so if we're past last week everything left can go
    if [[ past_week == 1 ]]; then
        hadoop fs -rm -r ${dir}
        continue
    fi

    # format path as datelike string YYYY-- YYYY-mm- YYYY-mm-dd
    date_string=$( echo ${dir} | sed -r "s|${LATEST_PATH}\/([0-9]{4})\/?([0-9]{2})?\/?([0-9]{2})?|\1-\2-\3|" )

    # get date in seconds from string
    if [[ ${date_string} =~ [0-9]--$ ]]; then
        # yearly directories are set to last day of that year
        date_string=$( echo ${date_string} | sed -r "s|-$||" )
        dir_date=$(date -d "${date_string}-01-01 +1 year -1 day" '+%s')
    elif [[ ${date_string} =~ [0-9]-$ ]]; then
        # monthly directories are set to last day of that month
        dir_date=$(date -d "${date_string}01 +1 month -1 day" '+%s')
    else
        # date is parsed as expected
        dir_date=$(date -d ${date_string} '+%s')
    fi

    # if folder is from before last week remove it
    if [[ ${dir_date} -lt ${last_week} ]]; then
        hadoop fs -rm -r ${dir}
        past_week=1
    fi
done
