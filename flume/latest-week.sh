#!/bin/bash

mkdir -p 2021/12/04 2015/11/05 2019/12/29 2021/12/03
touch 2021/12/04/a.log
touch 2015/11/05/b.log

# delete all but last 7 days of logs
#last_week=$(date -d "last week 13:00" '+%Y-%m-%d')
last_week=$(date -d "last week 13:00" '+%s')

#`hadoop fs -ls /path/to/directory | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`

#'hadoop fs -ls -R /ecoFlume/latest | grep '^d' | sort -r'
# iterate through folders in descending order
for dir in $(find . -type d | sort -r | grep [0-9]$ )
do
    # remove directory if empty
    if [[ $(ls -A ${dir} | wc -l) == 0 ]]; then
        rmdir ${dir}
        echo "${dir} removed"
    else
        # format path as datelike string YYYY-- YYYY-mm- YYYY-mm-dd
        date_string=$(echo ${dir} | sed -r 's|\.\/([0-9]{4})\/?([0-9]{2})?\/?([0-9]{2})?|\1-\2-\3|')

        # if non-empty monthly directory continue on to next folder
        if [[ ${date_string} =~ [0-9]{1}-$ ]]; then
            continue
        fi

        # get date in seconds from string
        dir_date=$(date -d ${date_string} '+%s')

        # if folder is from before last week remove it
        if [[ ${dir_date} -lt ${last_week} ]]; then
            rm -r ${dir}
            echo "${dir} and contents removed"
        fi
    fi
done
