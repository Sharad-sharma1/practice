import pandas as pd
import os
ok = {"a":[1,2,3,4], "b":["f", "k", "j", "c"], "p":(7,8,9,20)}

df = pd.DataFrame(ok)

# os.mkdir("./lect_6")
df.to_csv("./lect_6/new.csv")

#if i dont want 1st index column
df.to_csv("./lect_6/new1.csv", index=False)

#if i want my own column name rather then orignal df key name.
df.to_csv("./lect_6/new2.csv", index=False, header=["sharad", "rahul", "dindayal"])