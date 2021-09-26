#!/bin/bash  
#Levo la data (anno-mese-giorno) dalla colonna timestamp, così ci lasciamo solo ore-minuti-secondi

#Gennaio
for i in {01..31}
    do
    to_delete="2016-01-${i} "
    filename="../../data/jan2016/cleaned/${i}.01.2016_sorted.csv"
    sed "s/${to_delete}//g" $filename > ../../data/jan2016/${i}.01.2016_cleaned.csv
done

#Febbraio
for i in {01..29}
    do
    to_delete="2016-02-${i} "
    filename="../../data/feb2016/cleaned/${i}.02.2016_sorted.csv"
    sed "s/${to_delete}//g" $filename > ../../data/feb2016/${i}.02.2016_cleaned.csv
done

#Marzo
for i in {01..31}
    do
    to_delete="2016-03-${i} "
    filename="../../data/mar2016/cleaned/${i}.03.2016_sorted.csv"
    sed "s/${to_delete}//g" $filename > ../../data/mar2016/${i}.03.2016_sorted.csv
done