import pandas as pd

##### 1 ######
data = [["tom", 10], ["nick", 15], ["juli", 14]]
df = pd.DataFrame(data, columns=["Name", "Age"])

###### 2 ######

data = {"calories": [420, 380, 390], "duration": [50, 40, 45]}
df = pd.DataFrame(data)

# nombre del index
df = pd.DataFrame(data, index=["day1", "day2", "day3"])

print(df)
