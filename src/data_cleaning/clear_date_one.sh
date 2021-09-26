#!/bin/bash  

# Implementazione non iterativa di clear-date_all.sh
index="12"
to_delete="2016-01-12 "
filename="../data/jan2016/12.01.2016.csv"
sed "s/2016-01-${index} //g" $filename > ../data/jan2016/${index}.01.2016_sorted.csv
