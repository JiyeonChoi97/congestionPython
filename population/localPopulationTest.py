import pandas as pd
import datetime
from pyarrow import csv

# 집계구
start_time4 = datetime.datetime.now()
smgu_cd = 1121063020301  # 집계구 코드
adstrd_code = ""              # 행정동 코드

# 집계구 - 내국인
smgu_local_list = pd.DataFrame()  # 집계구별 데이터 담는 Df
start_time = datetime.datetime.now()
filepath = 'LOCAL_PEOPLE_20190215'
convert_opts = csv.ConvertOptions(include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정
localDf = csv.read_csv(
    '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(단기체류 외국인)/TEMP_FOREIGNER_201902/TEMP_FOREIGNER_20190215.csv'.format(
        filepath[:19], filepath),
    read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
    convert_options=convert_opts).to_pandas()

local_row = localDf.loc[localDf['f3'] == smgu_cd]  # 집계구 내국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기

# 집계구별 일별 데이터의 개수가 24개가 아닌경우 비어있는 시간대 데이터값 -1 채워주기
if len(local_row) < 24:
    for i in range(24):
        if local_row.loc[local_row['f1'] == i].empty:
            data = {'f0': int(filepath[13:21]), 'f1': i, 'f2': adstrd_code, 'f3': smgu_cd, 'f4': -1}
            local_row = local_row.append(data, ignore_index=True)

# 시간대구분 기반으로 정렬하기
local_row = local_row.sort_values(by=['f1'])

smgu_local_list = pd.concat([smgu_local_list, local_row])  # 집계구별 데이터 담는 Df에 일별 데이터 추가

# print('-------------------------집계구 : {0} - 파일명 : {1}완료 -----------------------------'.format(smgu_cd, filepath))
del localDf         # DF 초기화
del local_row       # DF 초기화
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(" 내국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))

# 데이터 합치기
smgu_local_list.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화
smgu_local_list.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/test/{0}_단기외국인_test.csv'.format(smgu_cd), mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
del smgu_local_list  # 데이터프레임 초기화

end_time4 = datetime.datetime.now()
elapsed_time4 = end_time4 - start_time4
print(smgu_cd, " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time4, end_time4, elapsed_time4))

