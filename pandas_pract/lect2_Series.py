import pandas as pd

data = [1,2,3,4,5]
s1 = pd.Series(data, index=['a', 'b', 'c', 'd', 'e'], dtype=float)
print(s1)

data_set = {'python_v':[1.1, 2.3, 4.6], 'company_name': ['abbott', 'google'], 'year_use': [2001, 2002, 2010]}
s2 = pd.Series(data_set)
print(s2)




#  why to use series when numpy is there for array 
# reasion is when we add 2 arrays and there id are not matching numpy thourghs error broadcast error but series not
data_set_1 = [1,2,3,4,5]
s1 = pd.Series(data_set_1, index=['a', 'b', 'c', 'd', 'e'], dtype=float)

data_set_2 = [1,2,3]
s2 = pd.Series(data_set_2, index=['a', 'b', 'c'])
print(s1+s2)

# we can also repeast same data for all index's
ok = pd.Series(45, index=['a', 'b', 'c'])
print(ok)
