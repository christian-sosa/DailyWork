import pandas as pd

# initialize list of lists
data = [["tom", 10], ["nick", 15], ["juli", 14]]

# Create the pandas DataFrame
df = pd.DataFrame(data, columns=["Name", "Age"])

data2 = [["tom", 11], ["nick", 11], ["juli", 11]]

# Create the pandas DataFrame
df2 = pd.DataFrame(data2, columns=["Name", "Age"])

df = df2  # .copy()

df2.Age[1] = 16
# print dataframe.
print(df)
