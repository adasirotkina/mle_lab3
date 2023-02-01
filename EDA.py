import pandas as pd
from matplotlib import pyplot as plt

data = pd.read_csv('data/products_short.csv', sep = '\t')
good_col = []
for c in data.columns:
    if len(data[c].dropna()) > 5000:
        good_col.append(c)

print(data[good_col].info())

print(good_col)