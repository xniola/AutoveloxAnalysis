#!/bin/bash

for i in {1..31}
do

    # faccio in modo che gli indici 1,2,3,...,9 diventino 01,02,03,...,09(padding). Poi da 10 a 31 niente padding 
    fn=$((i+1))
    if [ $fn -lt 10 ]
        then
        fn="0$fn"
    fi

    pat=$i
    if [ $pat -lt 10 ]
      then 
      pat="0$pat"
    fi
    # La riga che contiene questo pattern va eliminata
    pattern="03-${pat}"

    filename="../data/mar2016/original_data/${fn}.03.2016.csv"
    sed "/${pattern}/d" $filename > ../data/mar2016/no_altri_giorni/${fn}.03.2016.csv
done