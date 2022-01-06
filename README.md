<h1>관광지 혼잡도 데이터 전처리 과정</h1>

<h3>주데이터</h3>

<h5>1. 휴일여부, 축제여부 정리된 csv 뽑기</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;fstvMysql.py
<h5>2. 관광객 수, 혼잡도 정리된 csv 뽑기</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;travelCntTest3.py
<h5>3. Maindata 합치기</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;mainData.py
<h5>4. Maindata 인덱싱</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;mainDataIndexing.py



<h3>관광지별 거리계산</h3>

<h5>1. 관광지별 거리계산</h5>
&nbsp;&nbsp;&nbsp;&nbsp;sitsDistance.py



<h3>부데이터 - 서울생활인구(집계구)</h3>

<h5>1. 집계구 코드 DataFrame 추출</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/smguCd.py
<h5>2. 집계구 내국인 파일명 리스트 추출</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/localFileList.py
<h5>3. 파일명 리스트 추출</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/localFileList.py<br>
    &nbsp;&nbsp;&nbsp;&nbsp;population/longFornFileList.py<br>
    &nbsp;&nbsp;&nbsp;&nbsp;population/tempFornFileList.py<br>
<h5>4. 존재하지 않는 파일명 리스트 추출</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/notIncludeFileLIst.py
<h5>5. 집계구 코드만 따로 저장(19153개)</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/smguCd.py
<h5>6. 각 집계구 데이터에 없는 날짜 추출</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/notIncludeDateList.py
<h5>7. 집계구 생활인구 추출</h5>
    &nbsp;&nbsp;&nbsp;&nbsp;population/211201/smguPopulation.py
