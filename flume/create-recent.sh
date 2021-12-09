# create files of recent data

# add a positional argument to run this first on its own
if [[ $1 != "" ]]; then
    # for each text file
    for file in ./*.txt
    do 
        # sort data by date - takes forever!
        sort -k 3 ${file} > ${file}_sorted.tmp
        echo "${file} sorted by date"
    done
else
    # for each sorted text file
    for file in ./*_sorted.tmp
    do
        # get the last 7 days of data
        most_recent=$(tail -n2 ${file} | head -n1 | awk -F '\\s+' '{print $3}')
        week_ago=$(date -d "last week ${most_recent}" '+%Y-%m-%d')

        # takes a long time too
        awk "/${week_ago}/,/*/" ${file} > ${file}_thatweek.tmp
        echo "Got last 7 days of data from ${file}"

        # change dates to those of this last week
        for i in {0..6}
        do
            # go a few days ahead so data isn't lost while we work with it
            this_day=$(date -d "$((i+3)) day" '+%Y-%m-%d')
            that_day=$(date -d "-$i day ${most_recent}" '+%Y-%m-%d')
            sed -n -i.bak "s/${that_day}/${this_day}" ${file}_thatweek.tmp        
        done

        # delete last line
        sed -n -i '$d' ${file}_thatweek.tmp

        # rename file
        mv ${file}_thatweek.tmp ${file}_week.new
        echo "Created ${file}_week.new"

        # clean up
        #rm *.tmp *.bak
    done
fi
