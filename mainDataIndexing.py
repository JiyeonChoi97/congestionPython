import pymysql  # pymysql 임포트
import pandas as pd
import numpy as np
import datetime

# 메인 코드
conn = pymysql.connect(host='222.117.10.195', port=33306, user='kocca', password='!@#kocca#@!', db='kocca', charset='utf8')  # 접속정보
cur = conn.cursor()  # 커서생성

# 관광지명 뽑아오기
sql = """
        select contentId, title
        from tar_deco_info tdi
        inner join (
            select poi_id
            from kt_di_kdap_native_travl_poi_in_sum
            group by poi_id
            ) ktn on ktn.poi_id = tdi.contentId
        group by contentId
       """

cur.execute(sql)  # 커서로 sql문 실행
num = 0
while (True):  # 반복실행
    num = num +1
    start_time = datetime.datetime.now()

    row = cur.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
    if row == None:  # 커서(테이블 셀렉트)에 더이상 값이 없으면
        break  # while문을 빠져나감

    nm = row[1].replace("/", "&")

    # 메인데이터 csv 파일 읽기
    main_data = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/주데이터/maindata_인덱싱전/{0}_주데이터.csv'.format(nm))

    # 메인데이터 인덱싱 추가
    main_data["index"] = np.arange(1, 6577, 1)
    print("-------------------------------- {0} - {1} 메인데이터 : ----------------------------------".format(num, nm))

    # 메인데이터 컬럼 순서 정렬
    main_data = main_data[['index', 'base_ymd', 'year', 'month', 'day', 'week', 'timezn', 'holiday_yn', 'poi_id', 'fstv_yn', 'in_native_cnt', 'native_cnt', 'forn_cnt', 'tot_cnt', 'cncrtAccPsonNum', 'congestion']]

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time

    print(row[1] + " 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))
    main_data.to_csv('{0}_주데이터.csv'.format(nm), mode="w", header=True, index=False)  # 데이터프레임 csv 파일로 저장
    del main_data  # 데이터프레임 초기화

print("--------------- maindata 완료---------------")

conn.close()  # 종료
