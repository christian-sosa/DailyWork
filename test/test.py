import os

# import cchc_commons
import awswrangler as wr
import pandas as pd

print(os.getenv("PASSWORD"))

# print(os.getenv("AMBIENTE"))


x = [1, 2, 3]
y = x
y[1] = 4

numero = 4

mydict = [
    {"a": "probando_probando2_probando3", "b": 2, "c": 3, "d": 4},
    {"a": "10000", "b": 200, "c": 300, "d": 400},
    {"a": "1000", "b": 2000, "c": 3000, "d": 4000},
]
df = pd.DataFrame(mydict)

# print(df.a.str.split("_").str[0])

# read = wr.s3.read_csv('s3://csosa-backup/SIP_221006_E.csv')
# print(read)
