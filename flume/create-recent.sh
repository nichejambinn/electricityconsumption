# create files of recent data
# 1 sorts
# 2 gets last 7 days of data from file
# 3 changes dates to this past week
# 4 split the file and give a name with timestamp

if [[ $1 == 1 ]]; then
    echo "Sorting data by date"
    # for each text file
    for file in ./*.txt
    do 
        # sort data by date - takes forever!
        sort -k 3 ${file} > ${file}_sorted.tmp
        echo "${file} sorted by date"
    done
elif [[ $1 == 2 ]]; then
    echo "Getting last 7 days of data"
    # for each sorted text file
    for file in ./*_sorted.tmp
    do
        # get the last 7 days of data
        most_recent=$(tail -n2 ${file} | head -n1 | awk -F '\\s+' '{print $3}')
        week_ago=$(date -d "last week ${most_recent}" '+%Y-%m-%d')

        # takes a long time too
        awk "/${week_ago}/,/*/" ${file} > ${file}_thatweek.tmp
        echo "Got last 7 days of data from ${file}"
    done
elif [[ $1 == 3 ]]; then
    echo "Changing dates"
    for file in ./*_thatweek.tmp
    do
        cp ${file} ${file}_thisweek.tmp

        # delete last line
        sed -i '$d' ${file}_thisweek.tmp

        most_recent=$(tail -n1 ${file}_thisweek.tmp | awk -F '\\s+' '{print $3}')
        # change dates to those of this past week
        for i in {0..7}
        do
            # go a few days ahead so data isn't lost while we work with it
            this_day=$(date -d "$((3-i)) day" '+%Y-%m-%d')
            that_day=$(date -d "-$i day ${most_recent}" '+%Y-%m-%d')
            sed -i.bak "s/${that_day}/${this_day}/" ${file}_thisweek.tmp        
        done

        echo "Created ${file}_thisweek.tmp with now recent data"
    done
else
    echo "Partitioning files"
    for file in ./*_thisweek.tmp
    do
        midpoint=$(($(wc -l ${file} | cut -d' ' -f1) / 2))

        # split up file
        sed -n "1,$((${midpoint}-1)) p" ${file} > ${file}_1.tmp
        sed -n "${midpoint},$ p" ${file} > ${file}_2.tmp
        
        # rename each file with timestamp of oldest record _yyyymmddhhmmss
        for splitfile in ./${file}_{1..2}.tmp
        do
            date_string=$(head -n1 ${splitfile} | awk -F '\\s+' '{print $3} {print $4}')
            oldest=$(date -d "${date_string}" '+%Y%m%d%H%M%S')
            filename=$( echo ${splitfile} | sed -r "s/.*(consumption_[0-9]+).*/\1_${oldest}.txt/")
            mv ${splitfile} ${filename}
            echo "Created ${filename}"
        done
        
        rm *.tmp *.bak
    done
fi
