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
for key, smguCode in smguCd.iterrows():
    start_time4 = datetime.datetime.now()
    smgu_cd = smguCode.values[0]  # 집계구 코드
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

        local_row = localDf.loc[localDf['f3'] == smgu_cd]  # 집계구 내국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기

        if key2 == 0:
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

        # print('-------------------------집계구 : {0} - 파일명 : {1}완료 -----------------------------'.format(smgu_cd, filepath))
        del localDf         # DF 초기화
        del local_row       # DF 초기화
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

        long_forn_row = longFornDf.loc[longFornDf['f3'] == smgu_cd]  # 집계구 장기체류 외국인 데이터에서 특정 값(집계구코드)을 가진 행 찾기

        # 집계구별 일별 데이터의 개수가 24개가 아닌경우 비어있는 시간대 데이터값 -1 채워주기
        if len(long_forn_row) < 24:
            for i in range(24):
                if long_forn_row.loc[long_forn_row['f1'] == i].empty:
                    data = {'f0': int(filepath[15:23]), 'f1': i, 'f2': 0, 'f3': smgu_cd, 'f4': -1}
                    long_forn_row = long_forn_row.append(data, ignore_index=True)
        # 시간대구분 기반으로 정렬하기
        long_forn_row = long_forn_row.sort_values(by=['f1'])

        smgu_long_forn_list = pd.concat([smgu_long_forn_list, long_forn_row])  # 집계구별 데이터 담는 Df에 일별 데이터 추가

        # print('-------------------------집계구 : {0} - 파일명 : {1}완료 -----------------------------'.format(smgu_cd, filepath))
        del longFornDf  # DF 초기화
        del long_forn_row  # DF 초기화
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
    smgu_local_list.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화
    smgu_long_forn_list.reset_index(drop=True, inplace=True)        # DF 인덱스 정렬 초기화
    smgu_temp_forn_list.reset_index(drop=True, inplace=True)        # DF 인덱스 정렬 초기화
    main_data = pd.concat([smgu_local_list, smgu_long_forn_list['f4'], smgu_temp_forn_list['f4']], axis=1)
    main_data.columns = ['STDR_DE_ID', 'TMZON_PD_SE', 'ADSTRD_CODE_SE', 'SMGU_CD', 'NATIVE_TOT', 'LONG_FORN_TOT', 'TEMP_FORN_TOT']

    # 빠진 날짜 데이터 -1로 채우기
    local_miss = pd.DataFrame()
    for miss_dt in miss_dt_list:
        for i in range(24):
            data = {'STDR_DE_ID': int(miss_dt), 'TMZON_PD_SE': i, 'ADSTRD_CODE_SE': adstrd_code, 'SMGU_CD': smgu_cd, 'NATIVE_TOT': -1, 'LONG_FORN_TOT': -1, 'TEMP_FORN_TOT': -1}
            local_miss = local_miss.append(data, ignore_index=True)
    main_data = pd.concat([main_data, local_miss])

    # 날짜 기반으로 정렬하기
    main_data = main_data.sort_values(by=['STDR_DE_ID', 'TMZON_PD_SE'])

    main_data.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/집계구/{0}.csv'.format(smgu_cd), mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
    del smgu_local_list  # 데이터프레임 초기화
    del smgu_long_forn_list  # 데이터프레임 초기화
    del smgu_temp_forn_list  # 데이터프레임 초기화
    del main_data  # 데이터프레임 초기화

    end_time4 = datetime.datetime.now()
    elapsed_time4 = end_time4 - start_time4
    print(smgu_cd, " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time4, end_time4, elapsed_time4))

