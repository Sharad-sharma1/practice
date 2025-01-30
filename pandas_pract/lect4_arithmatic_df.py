import pandas as pd

df1 = [
    (1,2,3,4),
    (4,6,7,1)
]

df1 = pd.DataFrame(df1, columns=["a", "b", "c", "d"])
df1["sum_of_ad"] = df1["a"]+df1["d"]
df1["sub_of_bc"] = df1["b"]-df1["c"]
df1["div_of_bc"] = df1["a"]/df1["b"]
df1["mui_of_bc"] = df1["c"]*df1["d"]
print(df1)


df2 = {"age": [23,24,25,26,44], "name":["sharad", "rahul", "priya", "ajay", "arvind"]}
df2 = pd.DataFrame(df2)
df2["age_mor_25"] = df2["age"] >= 25
df2["age_40+"] = df2["age"] >= 40
print(df2)
for i in df2["age_40+"]:
    if i:
        print('---', i)
