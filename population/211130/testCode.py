import pandas as pd
import datetime
from pyarrow import csv
import csv
from multiprocessing import Process, Queue

localFileName = pd.read_csv(
    '/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_내국인_파일명.csv')
longFornFileName = pd.read_csv(
    '/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_장기체류외국인_파일명.csv')
tempFornFileName = pd.read_csv(
    '/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구_단기체류외국인_파일명.csv')

# 내국인, 장외, 단외 별 데이터 없는 날짜
miss_dt_list = ['20191015', '20191016', '20191017', '20191018', '20191019', '20191020', '20191021', '20191022',
                '20191023', '20191024', '20191025', '20191026', '20191027']

def work(id, smgu_cd):
    print(id)
    start_time = datetime.datetime.now()
    # 집계구
    f = open('/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/smgu/집계구코드1111058030210.csv', 'w', encoding='utf-8')
    # wr = csv.writer(f)
    # wr.writerow([1, "Alice", True])
    # wr.writerow([2, "Bob", False])
    f.close()
    #
    #
    # convert_opts = csv.ConvertOptions(
    #     include_columns=['f0', 'f1', 'f2', 'f3', 'f4'])  # include_columns : 읽을 컬럼명만 지정
    # localDf = csv.read_csv(
    #     '/Users/choejiyeon/Documents/project/KOCCA/96.DB/부가데이터/서울생활인구/smgu/집계구코드1111058030210.csv',
    #     read_options=csv.ReadOptions(autogenerate_column_names=True, skip_rows=1),
    #     convert_options=convert_opts).to_pandas()
    #
    # print(localDf)
    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(id, " 내국인 시작시간 : {0}, 종료시간 : {1}, 걸린시간 : {2}".format(start_time, end_time, elapsed_time))




if __name__ == "__main__":
    # 집계구 코드 csv 파일 읽기
    smguCd = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/python/population/집계구코드.csv')

    print(len(smguCd))

    thList = []
    for i in range(1):
        th1 = Process(target=work, args=(i, smguCd.iloc[i][0]))
        thList.append(th1)

    for thread in thList:
        thread.start()

    for thread in thList:
        thread.join()
