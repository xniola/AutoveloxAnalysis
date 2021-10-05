#!/bin/bash

fname="../../data/feb2016/01.02.2016_cleaned.csv"
day="2016-02-01"
header=1
timestamp_regex='[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'

first=$(sed '2q;d' $fname)
[[ $first =~ $timestamp_regex ]]
pre_timestamp=$(date --date "$day ${BASH_REMATCH[0]}" +%s)

while read -r line; do

    if [ $header -eq 0 ]; then

        [[ $line =~ $timestamp_regex ]]

        timestamp=$(date --date "$day ${BASH_REMATCH[0]}" +%s)

        if [ $timestamp -ne $pre_timestamp ]; then
            delta=$((timestamp - pre_timestamp))
            
            sleep 0.1
            pre_timestamp=$timestamp
        fi

        echo "$line"

    else

        header=0
    fi

done <"$fname"
