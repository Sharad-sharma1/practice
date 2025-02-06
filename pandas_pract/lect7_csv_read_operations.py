import pandas as pd

# # to get top 3 or more rows
# csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=3)
# print(csv_data_set)


# # to fetch specific columns with names and indexing
# csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, 
#                            usecols=["industry_code_ANZSIC", "variable"])
# print(csv_data_set)

# csv_data_set = pd.read_csv("./lect_7/annual-enterpriaqdase-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, 
#                            usecols=[1,3])
# print(csv_data_set)


# # to skip specific rows
# csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, 
#                            usecols=["industry_code_ANZSIC", "variable"], skiprows=[1,3])
# print(csv_data_set)


# # give ur own heading or make a particular row as a heading - this is usefull when we dont get a columns in csv
# csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, 
#                            header=[1])
# print(csv_data_set)

# # note duplicate name not allowed 
# csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, 
#                            names=["ok", "ok1", "ok2", "ok3"])
# print(csv_data_set)


# note duplicate name not allowed 
csv_data_set = pd.read_csv("./lect_7/annual-enterprise-survey-2023-financial-year-provisional-size-bands.csv", nrows=4, dtype={"value":"float"})
print(csv_data_set)