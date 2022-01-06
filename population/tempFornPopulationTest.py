import pandas as pd
import datetime
from pyarrow import csv

# 집계구 코드 csv 파일 읽기
smguCd = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구코드.csv')
# localFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_내국인_파일명.csv')
# longFornFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_장기체류외국인_파일명.csv')
tempFornFileName = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_단기체류외국인_파일명.csv')

# 내국인, 장외, 단외 별 데이터 없는 날짜
miss_dt_list = ['20191015', '20191016', '20191017', '20191018', '20191019', '20191020', '20191021', '20191022', '20191023', '20191024', '20191025', '20191026', '20191027']

# 집계구
for key, smguCode in smguCd.iterrows():
    start_time4 = datetime.datetime.now()
    smgu_cd = smguCode.values[0]  # 집계구 코드
    adstrd_code = ""              # 행정동 코드

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

        temp_forn_row = tempFornDf.loc[tempFornDf['f3'] == smgu_cd]  # 집계구 단기체류 외국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기

        # 집계구별 일별 데이터의 개수가 24개가 아닌경우 비어있는 시간대 데이터값 -1 채워주기
        if len(temp_forn_row) < 24:
            for i in range(24):
                if temp_forn_row.loc[temp_forn_row['f1'] == i].empty:
                    data = {'f0': int(filepath[15:23]), 'f1': i, 'f2': 0, 'f3': smgu_cd, 'f4': -1}
                    temp_forn_row = temp_forn_row.append(data, ignore_index=True)
        # 시간대구분 기반으로 정렬하기
        temp_forn_row = temp_forn_row.sort_values(by=['f1'])

        smgu_temp_forn_list = pd.concat([smgu_temp_forn_list, temp_forn_row])  # 집계구별 데이터 담는 Df에 일별 데이터 추가

        # print('-------------------------집계구 : {0} - 파일명 : {1}완료 -----------------------------'.format(smgu_cd, filepath))
        del tempFornDf  # DF 초기화
        del temp_forn_row  # DF 초기화
    end_time3 = datetime.datetime.now()
    elapsed_time3 = end_time3 - start_time3
    print(" 단기체류 외국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time3, end_time3, elapsed_time3))

    # 데이터 합치기
    # smgu_local_list.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화
    # smgu_long_forn_list.reset_index(drop=True, inplace=True)        # DF 인덱스 정렬 초기화
    # smgu_temp_forn_list.reset_index(drop=True, inplace=True)        # DF 인덱스 정렬 초기화
    # main_data = pd.concat([smgu_local_list, smgu_long_forn_list['f4'], smgu_temp_forn_list['f4']], axis=1)

    # 빠진 날짜 데이터 -1로 채우기
    local_miss = pd.DataFrame()
    for miss_dt in miss_dt_list:
        for i in range(24):
            data = {'f0': int(miss_dt), 'f1': i, 'f2': adstrd_code, 'f3': smgu_cd, 'f4': -1}
            local_miss = local_miss.append(data, ignore_index=True)
    smgu_temp_forn_list = pd.concat([smgu_temp_forn_list, local_miss])

    # 날짜 기반으로 정렬하기
    smgu_temp_forn_list = smgu_temp_forn_list.sort_values(by=['f0', 'f1'])

    smgu_temp_forn_list.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/집계구/test/{0}.csv'.format(smgu_cd), mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
    # del smgu_local_list  # 데이터프레임 초기화
    # del smgu_long_forn_list  # 데이터프레임 초기화
    del smgu_temp_forn_list  # 데이터프레임 초기화

    end_time4 = datetime.datetime.now()
    elapsed_time4 = end_time4 - start_time4
    print(smgu_cd, " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time4, end_time4, elapsed_time4))

