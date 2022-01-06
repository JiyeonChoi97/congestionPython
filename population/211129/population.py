import pandas as pd
import datetime
from pyarrow import csv
from multiprocessing import Process, Queue

# 집계구 코드 csv 파일 읽기
smguCd = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구코드.csv')
localFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_내국인_파일명.csv')
longFornFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_장기체류외국인_파일명.csv')
tempFornFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_단기체류외국인_파일명.csv')

# 내국인, 장외, 단외 별 데이터 없는 날짜
miss_dt_list = ['20191015', '20191016', '20191017', '20191018', '20191019', '20191020', '20191021', '20191022', '20191023', '20191024', '20191025', '20191026', '20191027']

# 집계구 - 내국인
smgu_local_list = pd.DataFrame()  # 집계구별 데이터 담는 Df
# 집계구 - 장기체류 외국인
smgu_long_forn_list = pd.DataFrame()  # 집계구별 장기체류 외국인 데이터 담는 Df
# 집계구 - 단기체류 외국인
smgu_temp_forn_list = pd.DataFrame()  # 집계구별 단기체류 외국인 데이터 담는 Df

for key2, fileName in localFileName.iterrows():
    filepath = fileName.values[0]
    convert_opts = csv.ConvertOptions(include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정
    localDf = csv.read_csv(
        '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(내국인)/{0}/{1}'.format(
            filepath[:19], filepath),
        read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
        convert_options=convert_opts).to_pandas()

    # 집계구
    for key, smguCode in smguCd.iterrows():
        smgu_cd = smguCode.values[0]  # 집계구 코드
        local_row = localDf.loc[localDf['f3'] == smgu_cd]  # 집계구 내국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기
        adstrd_code = local_row.iloc[0, 2]   # 행정동 코드

        # 집계구별 일별 데이터의 개수가 24개가 아닌경우 비어있는 시간대 데이터값 -1 채워주기
        if len(local_row) < 24:
            for i in range(24):
                if local_row.loc[local_row['f1'] == i].empty:
                    data = {'f0': int(filepath[13:21]), 'f1': i, 'f2': adstrd_code, 'f3': smgu_cd, 'f4': -1}
                    local_row = local_row.append(data, ignore_index=True)

        # 시간대구분 기반으로 정렬하기
        local_row = local_row.sort_values(by=['f1'])

        smgu_local_list = pd.concat([smgu_local_list, local_row])  # 집계구별 데이터 담는 Df에 일별 데이터 추가
        smgu_local_list.to_csv(
            '/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/{0}.csv'.format(smgu_cd),
            mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
        del local_row
    del localDf         # DF 초기화

