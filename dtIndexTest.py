import pandas as pd

dt_index = pd.date_range(start='20180101', end='20201231')
dt_list = dt_index.strftime("%Y%m%d").tolist()
timezn = ['01', '02', '03', '04', '05', '06']
for date in dt_list:
  for time in timezn:
    print(time +" : "+ date)
    print(type(date))