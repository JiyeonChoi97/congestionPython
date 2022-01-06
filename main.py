import pymysql  # pymysql 임포트
import csv

# 전역변수 선언부
conn = None
cur = None

sql = ""
sql2 = ""

# 메인 코드
conn = pymysql.connect(host='222.117.10.195', port=33306, user='kocca', password='!@#kocca#@!', db='kocca',
                       charset='utf8')  # 접속정보
cur = conn.cursor()  # 커서생성
cur2 = conn.cursor()  # 커서생성

sql = "select poi_id, poi_name from kt_poi"  # 실행할 sql문
sql2 = """
        select @ROWNUM := @ROWNUM + 1 as rownum, a.*
        from (
             select distinct base_ymd,
               year(base_ymd) as year,
               month(base_ymd) as month,
               day(base_ymd) as day,
               substr(_UTF8'일월화수목금토', dayofweek(base_ymd), 1 ) as week,
               if(kis.base_ymd = hi.holiday_dt or dayofweek(base_ymd) = 1 or dayofweek(base_ymd) = 7, 'Y', 'N') as holiday_yn,
               poi.poi_id,
               if(poi.poi_id = dasfa.sits_id, 'Y', 'N') as fstv_yn
            from kt_poi poi
            inner join dev_di_kdap_native_travl_ingrs_sum kis on 1=1
            left join holiday_info hi on kis.base_ymd = hi.holiday_dt
            left join dev_adm_sits_fstv_adm dasfa on poi.poi_id = dasfa.sits_id and kis.base_ymd between dasfa.fstv_st_ymd and dasfa.fstv_fns_ymd
            where poi.poi_id = %s
            order by base_ymd
                 ) a,
        (select @ROWNUM := 0 ) b
       """

result = cur.execute(sql)  # 커서로 sql문 실행

while (True):  # 반복실행
    row = cur.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
    if row == None:  # 커서(테이블 셀렉트)에 더이상 값이 없으면
        break  # while문을 빠져나감

    nm = row[1].replace("/", "&")

    # csv 초기화
    f = open('{0}.csv'.format(nm), 'w', newline='', encoding='utf-8')
    wr = csv.writer(f)

    headerList = ['index', 'base_ymd', 'year', 'month', 'day', 'week', 'holiday_yn', 'poi_id', 'fstv_yn']  # 헤더 생성
    wr.writerow(headerList)

    arr = []

    fstvResult = cur2.execute(sql2, (row[0]))  # 커서로 sql문 실행

    while (True):
        row2 = cur2.fetchone()
        if row2 == None:
            break

        fstvArr = []

        for item in row2:
            fstvArr.append(item)

        wr.writerow(fstvArr)

    print("---------------poi_name : ", row[1], "출력 완료---------------")

f.close()

conn.close()  # 종료
