import pymysql  # pymysql 임포트
import pandas as pd
import datetime

# 메인 코드
conn = pymysql.connect(host='222.117.10.195', port=33306, user='kocca', password='!@#kocca#@!', db='kocca', charset='utf8')  # 접속정보
cur = conn.cursor()  # 커서생성
cur2 = conn.cursor()  # 커서생성

# 관광지명 뽑아오기
sql = """
        select distinct kp.poi_id, tdi.title
        from kt_poi kp
        inner join tar_deco_info tdi on kp.poi_id = tdi.contentId
        order by kp.poi_id
       """

cur.execute(sql)  # 커서로 sql문 실행
num = 0

sitsDistance = pd.DataFrame()
sitsName_df = list()
while (True):  # 반복실행
    num = num +1
    start_time = datetime.datetime.now()

    row = cur.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
    if row == None:  # 커서(테이블 셀렉트)에 더이상 값이 없으면
        break  # while문을 빠져나감

    # 관광지별 거리 계산
    sql2 = """
            SELECT (3956 * 2 * ASIN(SQRT(POWER(SIN((sits1.origin_xcrd - sits2.origin_xcrd) * pi()/180 / 2), 2) +
                     COS(sits1.origin_xcrd * pi()/180) *  COS(sits2.origin_xcrd * pi()/180) *
                     POWER(SIN((sits1.origin_ycrd - sits2.origin_ycrd) * pi()/180 / 2), 2))) * 1609.344 ) / 1000 AS DISTANCE,
                   sits1.sits_id, sits1.title, sits2.sits_id, sits2.title
            FROM  (
                    select distinct kasa.sits_id, tdi.title, kasa.origin_xcrd, kasa.origin_ycrd
                    from (
                        select sits_id, origin_xcrd, origin_ycrd, max(base_ym)
                        from kt_adm_sits_adm
                        group by sits_id, base_ym
                        having base_ym = 202012
                             ) kasa
                    inner join tar_deco_info tdi on kasa.sits_id = tdi.contentId
                      ) sits1,
                 (
                 select kasa.sits_id, tdi.title, kasa.origin_xcrd, kasa.origin_ycrd, max(kasa.base_ym)
                 from kt_adm_sits_adm kasa
                 inner join tar_deco_info tdi on kasa.sits_id = tdi.contentId
                 where sits_id = %s and base_ym = 202012
                     ) sits2
            order by sits1.sits_id
            """

    cur2.execute(sql2, (row[0]))  # 커서로 sql문 실행

    distance_df = list()
    while (True):
        row2 = cur2.fetchone()  # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
        if row2 == None:  # 커서(테이블 셀렉트)에 더이상 값이 없으면
            break  # while문을 빠져나감
        # print(row2)

        if num == 1:
            sitsName_df.append(row2[1])

        distance_df.append(row2[0])
        # print(distance_df)

    sitsDistance = sitsDistance.append(pd.Series(distance_df), ignore_index=True)
    # print(sitsDistance)
    print(str(num) + "{0} 출력 완료 ".format(row[1]))

sitsDistance.index = sitsName_df
sitsDistance.columns = sitsName_df
print(sitsDistance)
sitsDistance.to_csv('관광지별거리.csv', mode="w", header=True, index=True, encoding='utf-8-sig')  # 데이터프레임 csv 파일로 저장
print("--------------- maindata 완료---------------")

conn.close()  # 종료
