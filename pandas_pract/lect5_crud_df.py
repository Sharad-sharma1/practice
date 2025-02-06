import pandas as pd

ok = {"a":[1,2,3,4], "b":["f", "k", "j", "c"], "p":(7,8,9,20)}
df = pd.DataFrame(ok)
print(df)

# insert
df.insert(3, "copy_a", df["a"])
print(df)

#below will add data given slicing index rest will be none
df.insert(3, "copy_a2", df["a"][0:3])

# there is another way
df["copy_b"] = df["b"][1:] 

#delete in df 
del_orignal_a = df.pop("a")
print(df)
print(del_orignal_a)

