
import pandas as pd 

#Gennaio


#Febbraio

df = pd.read_csv("../../data/01.02.2016.csv",delimiter=";") 

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(by="timestamp")

df.to_csv("../../data/01.02.2016_sorted.csv", index = False)
