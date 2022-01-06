import pymysql  # pymysql 임포트
import pandas as pd
import datetime

# 메인 코드
conn = pymysql.connect(host='222.117.10.195', port=33306, user='kocca', password='!@#kocca#@!', db='kocca', charset='utf8')  # 접속정보
cur = conn.cursor()  # 커서생성

# poi_id 뽑아오기
sql = """
        select contentId, title, cncrtAccPsonNum
        from tar_deco_info tdi
        inner join (
            select poi_id
            from kt_di_kdap_native_travl_poi_in_sum
            group by poi_id
            ) ktn on ktn.poi_id = tdi.contentId
        group by contentId
       """

cur.execute(sql)  # 커서로 sql문 실행

# 현지인, 내국인, 외국인 csv 파일 읽기
csv_innative = pd.read_csv('di_kdap_native_travl_poi_sex_age_in_sum_20211018.txt', sep='|')
print("-------------------------------- 현지인 csv 파일 : ----------------------------------")
print(csv_innative)
csv_native = pd.read_csv('di_kdap_native_travl_poi_sex_age_sum_20211018.txt', sep='|')
print("-------------------------------- 내국인 csv 파일 : ----------------------------------")
print(csv_native)
csv_forn = pd.read_csv('di_kdap_forn_travl_timezn_cntry_sum_20211018.txt', sep='|')
print("-------------------------------- 외국인 csv 파일 : ----------------------------------")
print(csv_forn)

while (True):  # 반복실행
    start_time = datetime.datetime.now()

    row = cur.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
    if row == None:  # 커서(테이블 셀렉트)에 더이상 값이 없으면
        break  # while문을 빠져나감

    congestion_df = pd.DataFrame(columns=['base_ymd', 'timezn', 'poi_id', 'in_native_cnt', 'native_cnt', 'forn_cnt', 'tot_cnt', 'cncrtAccPsonNum', 'congestion'])

    # poi_id별로 데이터프레임에 담기
    inNative_data = csv_innative.loc[(csv_innative['poi_id'] == str(row[0]))]
    native_data = csv_native.loc[(csv_native['poi_id'] == str(row[0]))]
    forn_data = csv_forn.loc[(csv_forn['poi_id'] == str(row[0]))]

    dt_index = pd.date_range(start='20180101', end='20201231')
    dt_list = dt_index.strftime("%Y%m%d").tolist()
    time_list = ['01', '02', '03', '04', '05', '06']
    for date in dt_list:
        for time in time_list:
            inNativeCnt = 0
            nativeCnt = 0
            fornCnt = 0
            totCnt = 0
            # csv_innative - 현지인 데이터에서 특정 값(날짜, 시간)을 가진 행 찾기
            inNative_row = inNative_data.loc[(inNative_data['base_ymd'] == int(date)) & (inNative_data['timezn_div_cd'] == int(time)), ['in_native_travl_cnt']]
            if not inNative_row.empty:
                inNativeCnt = inNative_row['in_native_travl_cnt'].sum()
            # csv_native - 내국인 데이터에서 특정 값(날짜, 시간)을 가진 행 찾기
            native_row = native_data.loc[(native_data['base_ymd'] == int(date)) & (native_data['timezn_div_cd'] == int(time)), ['native_travl_cnt']]
            if not native_row.empty:
                nativeCnt = native_row['native_travl_cnt'].sum()
            # csv_forn - 외국인 데이터에서 특정 값(날짜, 시간)을 가진 행 찾기
            forn_row = forn_data.loc[(forn_data['base_ymd'] == int(date)) & (forn_data['timezn_div_cd'] == int(time)), ['forn_travl_cnt']]
            if not forn_row.empty:
                fornCnt = forn_row['forn_travl_cnt'].sum()

            totCnt = inNativeCnt + nativeCnt + fornCnt
            congestion = float(totCnt) / float(row[2])

            if inNativeCnt == 0:
                inNativeCnt = -1
            if nativeCnt == 0:
                nativeCnt = -1
            if fornCnt == 0:
                fornCnt = -1
            data_to_insert = {'base_ymd': date, 'timezn': time, 'poi_id': row[0], 'in_native_cnt': inNativeCnt, 'native_cnt': nativeCnt, 'forn_cnt': fornCnt, 'tot_cnt': totCnt, 'cncrtAccPsonNum': row[2], 'congestion': congestion}
            congestion_df = congestion_df.append(data_to_insert, ignore_index=True)  # 데이터 추가해서 원래 데이터프레임에 저장하기

    print("---------------" + row[1] + "추출 완료---------------")
    nm = row[1].replace("/", "&")
    congestion_df.to_csv('{0}_관광객수.csv'.format(nm), mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
    del congestion_df  # 데이터프레임 초기화

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time

    print(row[1] + " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))

print("---------------관광객수, 혼잡도 완료---------------")

conn.close()  # 종료
