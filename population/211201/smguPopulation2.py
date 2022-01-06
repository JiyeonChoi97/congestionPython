import pandas as pd
import datetime
from pyarrow import csv as pycsv
import os.path
import csv

# 집계구 코드 csv 파일 읽기
smguCd = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구행정동코드.csv')

# 내국인, 장기외국인, 단기외국인 별 데이터 없는 날짜
miss_dt_list = ['20191015', '20191016', '20191017', '20191018', '20191019', '20191020', '20191021', '20191022', '20191023', '20191024', '20191025', '20191026', '20191027']

dt_index = pd.date_range(start='20180101', end='20201231')
for date in dt_index:
    print(date, "START~!")
    start_time = datetime.datetime.now()
    
    # start_time_1 = datetime.datetime.now()
    searchDate = str(date)[0:10].replace('-', '')
    convert_opts = pycsv.ConvertOptions(include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정

    # 파일 존재 여부 체크
    if os.path.exists('/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(내국인)/LOCAL_PEOPLE_{0}/LOCAL_PEOPLE_{1}.csv'.format(searchDate[:6], searchDate)):
        # 집계구 - 내국인
        localDf = pycsv.read_csv(
            '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(내국인)/LOCAL_PEOPLE_{0}/LOCAL_PEOPLE_{1}.csv'.format(
                searchDate[:6], searchDate),
            read_options=pycsv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
            convert_options=convert_opts).to_pandas()
    else:
        for key, smguCode in smguCd.iterrows():
            local_miss = pd.DataFrame()
            smgu_cd = smguCode.values[0]  # 집계구 코드
            adstrd_code = smguCode.values[1]  # 행정동 코드
            for i in range(24):
                data = {'STDR_DE_ID': int(searchDate), 'TMZON_PD_SE': i, 'ADSTRD_CODE_SE': adstrd_code,
                        'SMGU_CD': smgu_cd,
                        'NATIVE_TOT': -1, 'LONG_FORN_TOT': -1, 'TEMP_FORN_TOT': -1}
                local_miss = local_miss.append(data, ignore_index=True)

            # '집계구.csv' 에 데이터 쓰기
            f = open(
                '/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/Data/{0}.csv'.format(
                    smgu_cd), 'w', newline='')
            wr = csv.writer(f)
            wr.writerows(local_miss.values.tolist())
            f.close()

        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        print(searchDate, "시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))
        continue

        # '집계구.csv' 에 데이터 쓰기
        f = open(
            '/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/Data/{0}.csv'.format(smgu_cd), 'w', newline='')
        wr = csv.writer(f)
        wr.writerows(local_miss.values.tolist())
        f.close()
        continue

    # 집계구 - 장기체류 외국인
    longFornDf = pycsv.read_csv(
        '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(장기체류 외국인)/LONG_FOREIGNER_{0}/LONG_FOREIGNER_{1}.csv'.format(
            searchDate[:6], searchDate),
        read_options=pycsv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
        convert_options=convert_opts).to_pandas()

    # 집계구 - 단기체류 외국인
    tempFornDf = pycsv.read_csv(
        '/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(단기체류 외국인)/TEMP_FOREIGNER_{0}/TEMP_FOREIGNER_{1}.csv'.format(
            searchDate[:6], searchDate),
        read_options=pycsv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
        convert_options=convert_opts).to_pandas()
    
    # end_time_1 = datetime.datetime.now()
    # elapsed_time_1 = end_time_1 - start_time_1
    # print("1 구간 : {0}".format(elapsed_time_1))
    
    # 집계구
    for key, smguCode in smguCd.iterrows():
        # start_time_2 = datetime.datetime.now()
        
        smgu_cd = int(smguCode.values[0])  # 집계구 코드

        local_row = localDf.loc[localDf['f3'] == smgu_cd]  # 집계구 내국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기
        long_forn_row = longFornDf.loc[longFornDf['f3'] == smgu_cd]  # 집계구 장기체류 외국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기
        temp_forn_row = tempFornDf.loc[tempFornDf['f3'] == smgu_cd]  # 집계구 단기체류 외국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기

        adstrd_code = int(smguCode.values[1])  # 행정동 코드

        # end_time_2 = datetime.datetime.now()
        # elapsed_time_2 = end_time_2 - start_time_2
        # print("2 구간 : {0}".format(elapsed_time_2))
    
        # 집계구별 일별 데이터의 개수가 24개가 아닌경우 비어있는 시간대 데이터값 -1 채워주기
        # start_time_3 = datetime.datetime.now()
        if len(local_row) < 24:
            for i in range(24):
                if local_row.loc[local_row['f1'] == i].empty:
                    data = {'f0': searchDate, 'f1': i, 'f2': adstrd_code, 'f3': smgu_cd, 'f4': -1}
                    local_row = local_row.append(data, ignore_index=True)
        # if len(long_forn_row) < 24:
        #     for i in range(24):
        #         if long_forn_row.loc[long_forn_row['f1'] == i].empty:
        #             data = {'f0': searchDate, 'f1': i, 'f2': 0, 'f3': smgu_cd, 'f4': -1}
        #             long_forn_row = long_forn_row.append(data, ignore_index=True)
        # if len(temp_forn_row) < 24:
        #     for i in range(24):
        #         if temp_forn_row.loc[temp_forn_row['f1'] == i].empty:
        #             data = {'f0': searchDate, 'f1': i, 'f2': 0, 'f3': smgu_cd, 'f4': -1}
        #             temp_forn_row = temp_forn_row.append(data, ignore_index=True)

        # end_time_3 = datetime.datetime.now()
        # elapsed_time_3 = end_time_3 - start_time_3
        # print("3 구간 : {0}".format(elapsed_time_3))
        
        # start_time_4 = datetime.datetime.now()
        # 시간대구분 기반으로 정렬하기
        local_row = local_row.sort_values(by=['f1'])
        long_forn_row = long_forn_row.sort_values(by=['f1'])
        temp_forn_row = temp_forn_row.sort_values(by=['f1'])

        # DF 인덱스 정렬 초기화
        local_row.reset_index(drop=True, inplace=True)
        long_forn_row.reset_index(drop=True, inplace=True)
        temp_forn_row.reset_index(drop=True, inplace=True)

        # end_time_4 = datetime.datetime.now()
        # elapsed_time_4 = end_time_4 - start_time_4
        # print("4 구간 : {0}".format(elapsed_time_4))
        
        # start_time_5 = datetime.datetime.now()
        # 데이터 합치기
        main_data = pd.merge(local_row, long_forn_row, how="outer", on=['f0','f1','f2','f3'])
        main_data2 = pd.merge(main_data, temp_forn_row, how="outer", on=['f0','f1','f2','f3'])
        main_data2 = main_data2.fillna(-1)
        # main_data = pd.concat([local_row, long_forn_row['f4'], temp_forn_row['f4']], axis=1)
        main_data2.columns = ['STDR_DE_ID', 'TMZON_PD_SE', 'ADSTRD_CODE_SE', 'SMGU_CD', 'NATIVE_TOT', 'LONG_FORN_TOT', 'TEMP_FORN_TOT']

        # end_time_5 = datetime.datetime.now()
        # elapsed_time_5 = end_time_5 - start_time_5
        # print("5 구간 : {0}".format(elapsed_time_5))
        
        # start_time_6 = datetime.datetime.now()
        # '집계구.csv' 에 데이터 쓰기
        f = open('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/Data/{0}.csv'.format(smgu_cd), 'a', newline='')
        wr = csv.writer(f)
        headers = ['STDR_DE_ID', 'TMZON_PD_SE', 'ADSTRD_CODE_SE', 'SMGU_CD', 'NATIVE_TOT', 'LONG_FORN_TOT', 'TEMP_FORN_TOT']
        if int(searchDate) == 20180101:
            wr.writerow(headers)
        wr.writerows(main_data2.values.tolist())
        f.close()
        
        # end_time_6 = datetime.datetime.now()
        # elapsed_time_6 = end_time_6 - start_time_6
        # print("6 구간 : {0}".format(elapsed_time_6))

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(searchDate, "시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))