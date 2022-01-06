import pymysql  # pymysql 임포트
import csv


#csv 초기화
f = open('dev_adm_sits_fstv_adm_2.csv','w',newline='')
wr = csv.writer(f)

# 전역변수 선언부
conn = None
cur = None

sql=""
sql2=""

# 메인 코드
conn = pymysql.connect(host='222.117.10.195', port=33306, user='kocca', password='!@#kocca#@!', db='kocca', charset='utf8')     # 접속정보
cur = conn.cursor()     # 커서생성
cur2 = conn.cursor()    # 커서생성

sql = "select distinct sits_id, origin_xcrd, origin_ycrd from kt_adm_sits_adm"  # 실행할 sql문
sql2 = """
        select distinct kasa.sits_id, kasa.sits_nm, a.*
        from (
             select
                 (6371*acos(cos(radians(%s))*cos(radians(kafa.origin_ycrd))*cos(radians(kafa.origin_xcrd)
                -radians(%s))+sin(radians(%s))*sin(radians(kafa.origin_ycrd)))) AS distance,
                 kafa.fstv_id,
                 kafa.fstv_nm,
                 date_format(kafa.fstv_st_ymd, '%%Y%%m%%d')  as st,
                 date_format(kafa.fstv_fns_ymd, '%%Y%%m%%d') as fns
             FROM  kt_adm_fstv_adm kafa
             ) a
        inner join kt_adm_sits_adm kasa
        where
            a.fns >= '20180101' and a.st <= '20201231'
            and kasa.sits_id  = %s
            and a.distance <= 2
            """

result = cur.execute(sql)       # 커서로 sql문 실행

headerList = ['sits_id', 'sits_nm', 'sits_fstv_distance', 'fstv_id', 'fstv_nm', 'fstv_st_ymd', 'fstv_fns_ymd']
wr.writerow(headerList)

while (True) :  # 반복실행
    row = cur.fetchone()        # row에 커서(테이블 셀렉트)를 한줄 입력하고 다음줄로 넘어감
    if row== None :     # 커서(테이블 셀렉트)에 더이상 값이 없으면
        break   # while문을 빠져나감

    arr = []

    fstvResult = cur2.execute(sql2, (row[2], row[1], row[2], row[0]))   # 커서로 sql문 실행

    while (True) :
        row2 = cur2.fetchone()
        if row2==None :
            break

        fstvArr = []

        for item in row2:
            fstvArr.append(item)

        wr.writerow(fstvArr)

    print("---------------sits_name : ", row[1], "출력 완료---------------")
   # for item in row:
   #     arr.append(item)

   # print(arr)
    #wr.writerow(fstvArr)

f.close()


# conn.commit() # 저장

conn.close()    # 종료

