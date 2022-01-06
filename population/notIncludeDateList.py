import pandas as pd
import os
from pyarrow import csv

# 없는 파일 알아내기
path_dir = '/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/집계구/test/1107062080001.csv'

tempFornDf = csv.read_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/집계구/test/1107062080001.csv',read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1)).to_pandas()

dateList = pd.DataFrame()
dt_index = pd.date_range(start='20180101', end='20201231')

for date in dt_index:
    searchDate = int(str(date)[0:10].replace('-',''))
    if tempFornDf.loc[tempFornDf['f0'] == searchDate].empty:
        print(searchDate)
