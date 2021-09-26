
import pandas as pd 

#Gennaio

for i in range (1,32):
    index = i
    if i < 10:
        index="0"+str(i)

    df = pd.read_csv("../data/jan2016/cleaned/"+str(index)+".01.2016.csv",delimiter=";") 

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by="timestamp")

    df.to_csv("../data/jan2016/cleaned_and_sorted/"+str(index)+".01.2016_sorted.csv", index = False)


#Febbraio
for i in range (1,30):
    index = i
    if i < 10:
        index="0"+str(i)

    df = pd.read_csv("../data/feb2016/cleaned/"+str(index)+".02.2016.csv",delimiter=";") 

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by="timestamp")

    df.to_csv("../data/feb2016/cleaned_and_sorted/"+str(index)+".02.2016_sorted.csv", index = False)

#Marzo
for i in range (1,32):
    index = i
    if i < 10:
        index="0"+str(i)

    df = pd.read_csv("../data/mar2016/cleaned/"+str(index)+".03.2016.csv",delimiter=";") 

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by="timestamp")

    df.to_csv("../data/mar2016/cleaned_and_sorted/"+str(index)+".03.2016_sorted.csv", index = False)