#!/bin/bash

if [[ $1 != "" ]]; then 
    touch {a..d}.log
    hadoop fs -mkdir /test/2015/04/17 /test/2016/09/2{7..8} /test/2021/12/0{5..7} /test/2019/11/02
    hadoop fs -copyFromLocal a.log /test/2016/09/28
    hadoop fs -copyFromLocal b.log /test/2021/12/06
    hadoop fs -copyFromLocal c.log /test/2019/11/02
    hadoop fs -copyFromLocal d.log /test/2021/12/05
    rm {a..d}.log
    LATEST_PATH="/test/"
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
