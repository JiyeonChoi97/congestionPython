import pandas as pd
import datetime
from pyarrow import csv

# 집계구 코드 csv 파일 읽기
smguCd = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구코드.csv')
localFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_내국인_파일명.csv')
longFornFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_장기체류외국인_파일명.csv')
tempFornFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_단기체류외국인_파일명.csv')

# 내국인, 장외, 단외 별 데이터 없는 날짜
miss_dt_list = ['20191015', '20191016', '20191017', '20191018', '20191019', '20191020', '20191021', '20191022', '20191023', '20191024', '20191025', '20191026', '20191027']

# 집계구
start_time4 = datetime.datetime.now()
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

    smgu_local_list = pd.concat([smgu_local_list, localDf])  # 집계구별 데이터 담는 Df에 일별 데이터 추가
    del localDf         # DF 초기화

end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(" 내국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))

# 집계구 - 장기체류 외국인
smgu_long_forn_list = pd.DataFrame()  # 집계구별 장기체류 외국인 데이터 담는 Df
start_time2 = datetime.datetime.now()
for key2, fileName in longFornFileName.iterrows():
    filepath = fileName.values[0]
    convert_opts = csv.ConvertOptions(include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정
    longFornDf = csv.read_csv(
        '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(장기체류 외국인)/{0}/{1}'.format(
            filepath[:21], filepath),
        read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
        convert_options=convert_opts).to_pandas()

    smgu_long_forn_list = pd.concat([smgu_long_forn_list, longFornDf])  # 집계구별 데이터 담는 Df에 일별 데이터 추가

    del longFornDf  # DF 초기화
end_time2 = datetime.datetime.now()
elapsed_time2 = end_time2 - start_time2
print(" 장기체류 외국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time2, end_time2, elapsed_time2))

# 집계구 - 단기체류 외국인
smgu_temp_forn_list = pd.DataFrame()  # 집계구별 단기체류 외국인 데이터 담는 Df
start_time3 = datetime.datetime.now()
for key2, fileName in tempFornFileName.iterrows():
    filepath = fileName.values[0]
    convert_opts = csv.ConvertOptions(
        include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정
    tempFornDf = csv.read_csv(
        '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(단기체류 외국인)/{0}/{1}'.format(
            filepath[:21], filepath),
        read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
        convert_options=convert_opts).to_pandas()

    smgu_temp_forn_list = pd.concat([smgu_temp_forn_list, tempFornDf])  # 집계구별 데이터 담는 Df에 일별 데이터 추가

    del tempFornDf  # DF 초기화
end_time3 = datetime.datetime.now()
elapsed_time3 = end_time3 - start_time3
print(" 단기체류 외국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time3, end_time3, elapsed_time3))

smgu_local_list.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화
smgu_long_forn_list.reset_index(drop=True, inplace=True)        # DF 인덱스 정렬 초기화
smgu_temp_forn_list.reset_index(drop=True, inplace=True)        # DF 인덱스 정렬 초기화

end_time4 = datetime.datetime.now()
elapsed_time4 = end_time4 - start_time4
print( " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time4, end_time4, elapsed_time4))

smgu_local_list.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/smgu_cd/집계구내국인.csv',mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
smgu_long_forn_list.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/smgu_cd/집계구장기외국인.csv',mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
smgu_temp_forn_list.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/smgu_cd/집계구단기외국인.csv',mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장