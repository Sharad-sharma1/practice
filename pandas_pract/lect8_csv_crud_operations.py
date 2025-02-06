import pandas as pd


csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4) # dtype={"industry_code_ANZSIC", "str"})
# print(csv_data_set)
# ok = 

# print(csv_data_set.index)
# print(csv_data_set.columns)
# print(csv_data_set.describe())

# print(csv_data_set.head())
# print(csv_data_set.tail())
# print(csv_data_set[3:7])

# print(csv_data_set.index.array)

# this will make all data into array object we can use both ways
# print(csv_data_set.to_numpy()[1])
# import numpy as np
# print(np.asarray(csv_data_set))

# if u want to sort the data
# print(csv_data_set.sort_index(axis=0, ascending=False))

# update in pandas df

csv_data_set.loc[0,"industry_code_ANZSIC"] = "sharad"
print(csv_data_set)

# below will warning and it is not good practice to do above is better
# csv_data_set["industry_code_ANZSIC"][0] = 'A'
# print(csv_data_set)

#  we can also use iloc to update
csv_data_set.iloc[1,0] = "rejesh"
print(csv_data_set)


# to delete column or row 
ok = csv_data_set.drop("unit", axis=1)
print(ok)

ok = csv_data_set.drop(0, axis=0)
print(ok)