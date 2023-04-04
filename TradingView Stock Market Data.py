from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import datetime as dt 

#username = 'YourTradingViewUsername'
#password = 'YourTradingViewPassword'

tv = TvDatafeed()

extended_price_data = tv.get_hist(symbol="SBIN", exchange="NSE", interval=Interval.in_daily, n_bars=305, extended_session=False) 

# extended_price_data.to_csv("SBI.csv")

df = pd.DataFrame(extended_price_data)

df.reset_index(inplace=True)

df['Date'] = pd.to_datetime(df["datetime"]).dt.date 
df['Date'] = pd.to_datetime(df['Date'])

df["Month-Year"] = df['Date'].dt.strftime('%b-%Y') 

# print(df.columns)
# print(df)

df2 = df[['symbol', 'Date', 'Month-Year', 'open', 'high', 'low', 'close', 'volume']] 
# df2.to_csv("SBI.csv")

Month_Data = df2.groupby('Month-Year').agg({'high': 'max', 'low' : 'min'}) 
# print(Month_Data)

Month_Data.rename(columns = {'high' : "month_High",
                              'low' : "month_low"}, inplace = True) 
Month_Data.reset_index(inplace=True)

Month_Data['Monthly_change'] = Month_Data['month_High'] - Month_Data['month_low'] 

# print(Month_Data)

start = "2022-01-01"
end = "2023-03-01"
bussiness_days = pd.date_range(start, end, freq='BM')
# print(bussiness_days)

bussiness_days = ['2022-01-31', '2022-02-28', '2022-03-31', '2022-04-29',
               '2022-05-31', '2022-06-30', '2022-07-29', '2022-08-30',
               '2022-09-30', '2022-10-31', '2022-11-30', '2022-12-30',
               '2023-01-31', '2023-02-28', '2023-03-31']

bds = df2[df2['Date'].isin(bussiness_days)]

# print(bds)

data = pd.merge(bds, Month_Data, how= 'inner', on= 'Month-Year')
# print(data) 

Month_Avg_Change = data.groupby('symbol')['Monthly_change'].mean()
# print(Month_Avg_Change)

data['Month_Avg_Change'] = int(Month_Avg_Change)
# print(data)

# data['Open_close_diff'] = 
data.to_csv("SBI.csv")