#!/bin/bash  
# Implementazione non iterativa di clear-date_all.sh

to_delete="2016-02-01 " #Levo la data (anno-mese-giorno) dalla colonna timestamp, così ci lasciamo solo ore-minuti-secondi
filename="../../data/01.02.2016_sorted.csv"
sed "s/${to_delete}//g" $filename > ../../data/01.02.2016_cleaned.csv