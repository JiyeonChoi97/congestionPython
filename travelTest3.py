import pymysql  # pymysql 임포트
import pandas as pd
import datetime

# 현지인, 내국인, 외국인 csv 파일 읽기
csv_innative = pd.read_csv('di_kdap_native_travl_poi_sex_age_in_sum_20211018.txt', sep='|')
print("-------------------------------- 현지인 csv 파일 : ----------------------------------")
csv_native = pd.read_csv('di_kdap_native_travl_poi_sex_age_sum_20211018.txt', sep='|')
print("-------------------------------- 내국인 csv 파일 : ----------------------------------")
csv_forn = pd.read_csv('di_kdap_forn_travl_timezn_cntry_sum_20211018.txt', sep='|')
print("-------------------------------- 외국인 csv 파일 : ----------------------------------")


# poi_id 별로 데이터프레임에 담기
inNative_data = csv_innative.loc[(csv_innative['poi_id'] == str(1177751))]
native_data = csv_native.loc[(csv_native['poi_id'] == str(1177751))]
forn_data = csv_forn.loc[(csv_forn['poi_id'] == str(1177751))]

inNative_data = inNative_data.sort_values(by=['base_ymd'])
native_data = native_data.sort_values(by=['base_ymd'])
forn_data = forn_data.sort_values(by=['base_ymd'])

inNative_data = inNative_data.drop_duplicates(['base_ymd'])
native_data = native_data.drop_duplicates(['base_ymd'])
forn_data = forn_data.drop_duplicates(['base_ymd'])

inNative_data.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화
native_data.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화
forn_data.reset_index(drop=True, inplace=True)            # DF 인덱스 정렬 초기화

inNative_data = inNative_data[['poi_id', 'base_ymd']]

main_data = pd.concat([inNative_data, native_data['base_ymd'], forn_data['base_ymd']], axis=1)
main_data.columns = ['poi_id', 'inNative_base_ymd', 'native_base_ymd', 'forn_base_ymd']
main_data.to_csv('/Users/choejiyeon/Documents/project/KOCCA/96.DB/주데이터/서울특릭아이미술관_측정날짜.csv', mode="w", header=True, index=True)  # 데이터프레임 csv 파일로 저장
