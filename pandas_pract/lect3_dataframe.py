import pandas as pd

sk = [[1],[2],[3],[4],[5]]
sk = pd.DataFrame(sk)
print(sk)

sk = {'a': [1,2,3,4,5], 'b':[4,5,6,7,8], 'c':[4,5,6,7,8], 'e':[4,5,6,7,8]}
sk = pd.DataFrame(sk)
print(sk)
sk = pd.DataFrame(sk, columns=['b', 'e'])
print(sk)
print('---',sk['b'][4])


# 3 note that if the dict or list size is different then pandas will throw error

# but if u want to avoid that we use series below even both list has diferent lenth it will work also we can use series 
# for df creation
sk = {'a': pd.Series([1,2,3,4,5]), 'b': pd.Series([4,5,6,7])}
sk = pd.DataFrame(sk)
print(sk)