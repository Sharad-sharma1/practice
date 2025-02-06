import pandas as pd


# csv_data_set = pd.read_csv("./lect_8/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, usecols=["industry_code_ANZSIC","rme_size_grp","variable","value","unit"]) # dtype={"industry_code_ANZSIC", "str"})
csv_data_set = pd.read_csv("./lect_8/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv") # dtype={"industry_code_ANZSIC", "str"})
# print(csv_data_set)

#by using below we can drop all nan(empty) values from csv's
# csv_data_set = csv_data_set.dropna()
# print(csv_data_set)

#we drop row and columns wise too

# but note that if we pass 0 then it will remove all rows which has nan and if we use 1 then it will remove all the columns which has nan
# also to use belo we need to use and object else it does not work as expected
# csv_data_set = csv_data_set.dropna(axis=0)
# csv_data_set = csv_data_set.dropna(axis=1)
# print(csv_data_set)

# now in above the issue was it removes all row sand column even if there is only 1 nan
# so if we dont want this kind of thing then we can use below ways

# so if we use how-any it will remove all rows which has nan
# csv_data_set = csv_data_set.dropna(how="any")
# print(csv_data_set)

# so if we use how-all it will remove only those rows which whole row has nan
# csv_data_set = csv_data_set.dropna(how="all")
# print(csv_data_set)

# if we want particular value get deleted then we can use below
# csv_data_set = csv_data_set.dropna(subset=["value"])
# print(csv_data_set)


#remove all nan rows but the only difference is it create new data set
# we dont need to create and object
# csv_data_set.dropna(inplace=True)
# print(csv_data_set)

# now if we want to remove based on number of nan present then
# nan_counts = csv_data_set.isna().sum(axis=1)
# print("before",nan_counts)

# #still confuse how below works!
# csv_data_set = csv_data_set.dropna(thresh=1)  # Assign the modified DataFrame back
# print(csv_data_set)

# csv_data_set = csv_data_set.isna().sum(axis=1)
# print("after",csv_data_set)


# we can replace the nan with certain name
# csv_data_set = csv_data_set.fillna("sharad")
# print("after",csv_data_set)


# we can replace the nan with back data or forward data
# print("after", csv_data_set)
# csv_data_set = csv_data_set.bfill()
# csv_data_set = csv_data_set.ffill()
# print("after", csv_data_set)


# we can replace the nan but with limit for one row "n" nan will be replaced
print("after", csv_data_set)
csv_data_set = csv_data_set.fillna("sharad", limit=2)
print("after", csv_data_set)







