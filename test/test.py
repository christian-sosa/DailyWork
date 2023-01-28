import os

# import cchc_commons
import awswrangler as wr

# print(os.getenv("AWS_PROFILE"))

# print(os.getenv("AMBIENTE"))


x = [1, 2, 3]
y = x
y[1] = 4
print(y)

# read = wr.s3.read_csv('s3://csosa-backup/SIP_221006_E.csv')
# print(read)
