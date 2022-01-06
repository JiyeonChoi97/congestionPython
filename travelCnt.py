import pymysql  # pymysql 임포트
import pandas as pd

# 메인 코드
conn = pymysql.connect(host='222.117.10.195', port=33306, user='kocca', password='!@#kocca#@!', db='kocca', charset='utf8')  # 접속정보
cur = conn.cursor()  # 커서생성
cur2 = conn.cursor()  # 커서생성
cur3 = conn.cursor()  # 커서생성
cur4 = conn.cursor()  # 커서생성

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
sql2 = """
        select base_ymd, poi_id, ktz as timezn, sum(cnt) as in_native_cnt
        from (
            select tmp.*, if(tmp.atz = tmp.ktz, in_native_cnt, 0) as cnt
            from (
                select base_ymd, poi_id, a.timezn_div_cd as atz, kt.timezn_div_cd as ktz, sum(a.in_native_travl_cnt) as in_native_cnt
                from kt_di_kdap_native_travl_poi_sex_age_in_sum a
                left outer join kt_timezn kt on 1=1
                group by base_ymd, poi_id, a.timezn_div_cd, kt.timezn_div_cd
                having poi_id = %s
                order by base_ymd, a.timezn_div_cd, kt.timezn_div_cd
                     ) tmp
                 ) tmp2
        group by base_ymd, timezn
       """
sql3 = """
        select base_ymd, poi_id, ktz as timezn, sum(cnt) as native_cnt
        from (
            select tmp.*, if(tmp.atz = tmp.ktz, native_cnt, 0) as cnt
            from (
                select base_ymd, poi_id, a.timezn_div_cd as atz, kt.timezn_div_cd as ktz, sum(a.native_travl_cnt) as native_cnt
                from kt_di_kdap_native_travl_poi_sex_age_sum a
                left outer join kt_timezn kt on 1=1
                group by base_ymd, poi_id, a.timezn_div_cd, kt.timezn_div_cd
                having poi_id = %s
                order by base_ymd, a.timezn_div_cd, kt.timezn_div_cd
                     ) tmp
                 ) tmp2
        group by base_ymd, timezn
       """
sql4 = """
        select base_ymd, poi_id, ktz as timezn, sum(cnt) as forn_cnt
        from (
            select tmp.*, if(tmp.atz = tmp.ktz, forn_cnt, 0) as cnt
            from (
                select base_ymd, poi_id, a.timezn_div_cd as atz, kt.timezn_div_cd as ktz, sum(a.forn_travl_cnt) as forn_cnt
                from kt_di_kdap_forn_travl_timezn_cntry_sum a
                left outer join kt_timezn kt on 1=1
                group by base_ymd, poi_id, a.timezn_div_cd, kt.timezn_div_cd
                having poi_id = %s
                order by base_ymd, a.timezn_div_cd, kt.timezn_div_cd
                     ) tmp
                 ) tmp2
        group by base_ymd, timezn
       """

cur.execute(sql)  # 커서로 sql문 실행

while (True):  # 반복실행
    row = cur.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
    if row == None:  # 커서(테이블 셀렉트)에 더이상 값이 없으면
        break  # while문을 빠져나감

    df = pd.DataFrame(columns=['base_ymd', 'time', 'poi_id', 'in_native_cnt', 'native_cnt', 'forn_cnt', 'tot_cnt', 'cncrtAccPsonNum', 'congestion'])

    cur2.execute(sql2, (row[0]))  # 커서로 sql문 실행
    print("---------------poi_name : ", row[1], " 현지인 관광객 수 출력 완료---------------")
    cur3.execute(sql3, (row[0]))  # 커서로 sql문 실행
    print("---------------poi_name : ", row[1], " 내국인 관광객 수 출력 완료---------------")
    cur4.execute(sql4, (row[0]))  # 커서로 sql문 실행
    print("---------------poi_name : ", row[1], " 외국인 관광객 수 출력 완료---------------")

    # 현지인 관광객 수 채워 넣기
    while (True):
        row2 = cur2.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
        row3 = cur3.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
        row4 = cur4.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감

        if row2 == None:    # 커서(테이블 셀렉트)에 더이상 값이 없으면
            break   # while문을 빠져나감
        elif row3 == None:
            break
        elif row4 == None:
            break

        tz01 = ['01', '02', '03', '04', '05', '06']
        tz02 = ['07', '08', '09', '10', '11']
        tz03 = ['12', '13', '14']
        tz04 = ['15', '16', '17', '18']
        tz05 = ['19', '20', '21']
        tz06 = ['22', '23', '24']

        if row2[2] == '01':
            for timezone in tz01:
                data_to_insert = {'base_ymd': row2[0], 'time': timezone, 'poi_id': row2[1], 'in_native_cnt': row2[3], 'native_cnt': row3[3], 'forn_cnt': row4[3], 'tot_cnt': row2[3]+row3[3]+float(row4[3]), 'cncrtAccPsonNum': row[2], 'congestion': (row2[3]+row3[3]+float(row4[3]))/row[2]}
                df = df.append(data_to_insert, ignore_index=True)   # 데이터 추가해서 원래 데이터프레임에 저장하기
        elif row2[2] == '02':
            for timezone in tz02:
                data_to_insert = {'base_ymd': row2[0], 'time': timezone, 'poi_id': row2[1], 'in_native_cnt': row2[3], 'native_cnt': row3[3], 'forn_cnt': row4[3], 'tot_cnt': row2[3]+row3[3]+float(row4[3]), 'cncrtAccPsonNum': row[2], 'congestion': (row2[3]+row3[3]+float(row4[3]))/row[2]}
                df = df.append(data_to_insert, ignore_index=True)  # 데이터 추가해서 원래 데이터프레임에 저장하기
        elif row2[2] == '03':
            for timezone in tz03:
                data_to_insert = {'base_ymd': row2[0], 'time': timezone, 'poi_id': row2[1], 'in_native_cnt': row2[3], 'native_cnt': row3[3], 'forn_cnt': row4[3], 'tot_cnt': row2[3]+row3[3]+float(row4[3]), 'cncrtAccPsonNum': row[2], 'congestion': (row2[3]+row3[3]+float(row4[3]))/row[2]}
                df = df.append(data_to_insert, ignore_index=True)  # 데이터 추가해서 원래 데이터프레임에 저장하기
        elif row2[2] == '04':
            for timezone in tz04:
                data_to_insert = {'base_ymd': row2[0], 'time': timezone, 'poi_id': row2[1], 'in_native_cnt': row2[3], 'native_cnt': row3[3], 'forn_cnt': row4[3], 'tot_cnt': row2[3]+row3[3]+float(row4[3]), 'cncrtAccPsonNum': row[2], 'congestion': (row2[3]+row3[3]+float(row4[3]))/row[2]}
                df = df.append(data_to_insert, ignore_index=True)  # 데이터 추가해서 원래 데이터프레임에 저장하기
        elif row2[2] == '05':
            for timezone in tz05:
                data_to_insert = {'base_ymd': row2[0], 'time': timezone, 'poi_id': row2[1], 'in_native_cnt': row2[3], 'native_cnt': row3[3], 'forn_cnt': row4[3], 'tot_cnt': row2[3]+row3[3]+float(row4[3]), 'cncrtAccPsonNum': row[2], 'congestion': (row2[3]+row3[3]+float(row4[3]))/row[2]}
                df = df.append(data_to_insert, ignore_index=True)  # 데이터 추가해서 원래 데이터프레임에 저장하기
        else:
            for timezone in tz06:
                data_to_insert = {'base_ymd': row2[0], 'time': timezone, 'poi_id': row2[1], 'in_native_cnt': row2[3], 'native_cnt': row3[3], 'forn_cnt': row4[3], 'tot_cnt': row2[3]+row3[3]+float(row4[3]), 'cncrtAccPsonNum': row[2], 'congestion': (row2[3]+row3[3]+float(row4[3]))/row[2]}
                df = df.append(data_to_insert, ignore_index=True)  # 데이터 추가해서 원래 데이터프레임에 저장하기

    print("---------------Dataframe : ", df, "---------------")

    nm = row[1].replace("/", "&")
    df.to_csv('{0}_관광객수.csv'.format(nm), mode="w", header=True, index=False)    # 데이터프레임 csv 파일로 저장
    del df  # 데이터프레임 초기화
    print("---------------", row[1], "_관광객수.csv 완료---------------")

conn.close()  # 종료
