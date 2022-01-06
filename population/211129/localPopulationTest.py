import pandas as pd
import datetime
from pyarrow import csv

# 집계구 코드 csv 파일 읽기
smguCd = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구코드.csv')
localFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_내국인_파일명.csv')

# 내국인, 장외, 단외 별 데이터 없는 날짜
miss_dt_list = ['20191015', '20191016', '20191017', '20191018', '20191019', '20191020', '20191021', '20191022', '20191023', '20191024', '20191025', '20191026', '20191027']

# 집계구
start_time4 = datetime.datetime.now()
smgu_cd = 1121063020301  # 집계구 코드
adstrd_code = ""              # 행정동 코드

# 집계구 - 내국인
smgu_local_list = pd.DataFrame()  # 집계구별 데이터 담는 Df
start_time = datetime.datetime.now()
for key2, fileName in localFileName.iterrows():
    filepath = fileName.values[0]
    convert_opts = csv.ConvertOptions(include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정
    localDf = csv.read_csv(
        '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(내국인)/{0}/{1}'.format(
            filepath[:19], filepath),
        read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
        convert_options=convert_opts).to_pandas()

    local_row = localDf.loc[(localDf['f3'] == '1121063020301') & (localDf['f0'] == '20190215') & (localDf['f1'] == '10')]  # 집계구 내국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기


    # 집계구별 일별 데이터의 개수가 24개가 아닌경우 비어있는 시간대 데이터값 -1 채워주기
    if len(local_row) > 0:
        print(filepath)
        print(local_row)


    smgu_local_list = pd.concat([smgu_local_list, local_row])  # 집계구별 데이터 담는 Df에 일별 데이터 추가

    # print('-------------------------집계구 : {0} - 파일명 : {1}완료 -----------------------------'.format(smgu_cd, filepath))
    del localDf         # DF 초기화
    del local_row       # DF 초기화
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(" 내국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))

smgu_local_list.columns = ['STDR_DE_ID', 'TMZON_PD_SE', 'ADSTRD_CODE_SE', 'SMGU_CD', 'NATIVE_TOT']

# 빠진 날짜 데이터 -1로 채우기
local_miss = pd.DataFrame()
for miss_dt in miss_dt_list:
    for i in range(24):
        data = {'STDR_DE_ID': int(miss_dt), 'TMZON_PD_SE': i, 'ADSTRD_CODE_SE': adstrd_code, 'SMGU_CD': smgu_cd, 'NATIVE_TOT': -1}
        local_miss = local_miss.append(data, ignore_index=True)
smgu_local_list = pd.concat([smgu_local_list, local_miss])

# 날짜 기반으로 정렬하기
smgu_local_list = smgu_local_list.sort_values(by=['STDR_DE_ID', 'TMZON_PD_SE'])

smgu_local_list.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/test/{0}_내국인.csv'.format(smgu_cd), mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
del smgu_local_list  # 데이터프레임 초기화

end_time4 = datetime.datetime.now()
elapsed_time4 = end_time4 - start_time4
print(smgu_cd, " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time4, end_time4, elapsed_time4))

