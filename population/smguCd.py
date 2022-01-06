import pandas as pd

# 집계구 코드 따로 저장하기 - 19153개
# 메인데이터 csv 파일 읽기
main_data = pd.read_csv('/Users/choejiyeon/Documents/project/KOCCA/99.기타/Data/kocca_데이터_수집_파일/서울생활인구/집계구 단위(내국인)/LOCAL_PEOPLE_201801/LOCAL_PEOPLE_20180101.csv')
main_data = main_data.drop_duplicates(['집계구코드'])[['집계구코드', '행정동코드']]
print(len(main_data))

main_data.to_csv('집계구행정동코드.csv', mode="w", header=True, index=False, encoding='utf-8-sig')  # 데이터프레임 csv 파일로 저장

