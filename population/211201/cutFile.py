import pandas as pd
import numpy as np

chunksize = 10000
i = 0
link = '/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구행정동코드.csv'
link = link[1:]
df = pd.read_excel(link)
for chunk in np.array_split(df, len(df) // chunksize):
    chunk.to_excel('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구행정동코드_2.csv'.format(i), index=False)
    i = i + 1