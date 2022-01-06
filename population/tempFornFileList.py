import pandas as pd
import os

# 집계구 단기체류 외국인 폴더 리스트
path_dir = '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(단기체류 외국인)'

dir_list = os.listdir(path_dir)
sortedDirList = sorted(dir_list)
del sortedDirList[0]

df = pd.DataFrame()

# Dataframe for문
for value in sortedDirList:
    # print(value)

    # 집계구 단기체류 외국인 폴더 파일 리스트
    path_dir = '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(단기체류 외국인)/{0}'.format(value)

    file_list = os.listdir(path_dir)
    sortedFileList = sorted(file_list)
    df = df.append(sortedFileList)

print(df)
print(len(df))
print('---------------------------------')

df.to_csv('집계구_단기체류외국인_파일명.csv', mode="w", header=True, index=False, encoding='utf-8-sig')  # 데이터프레임 csv 파일로 저장

