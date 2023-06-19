import requests
import pandas as pd
from bs4 import BeautifulSoup
import yahoo_fin.stock_info as si
import CassandraAccess

# get list of Dow tickers
# OTHER TICKERS, SP500 does not work
# FULL DOCUMENTATION HERE - https://theautomatic.net/yahoo_fin-documentation/

dow_list = si.tickers_dow()[:2]
print(dow_list)
 
# Get data in the current column for each stock's valuation table
dow_stats = {}
cnt=0
for ticker in dow_list:
    cnt+=1
    # print(ticker,cnt)
    temp = si.get_stats_valuation(ticker)
    temp = temp.iloc[:,:2]
    temp.columns = ["Attribute", "Recent"]
 
    dow_stats[ticker] = temp
 
 
# combine all the stats valuation tables into a single data frame
combined_stats = pd.concat(dow_stats)
combined_stats = combined_stats.reset_index()
 
del combined_stats["level_1"]
 
# update column names
combined_stats.columns = ["Ticker", "Attribute", "Recent"]

print(combined_stats)

# for index, row in combined_stats.iterrows():
#     print(row['Ticker'], row['Attribute'])

ca = CassandraAccess.CassandraAccess()
ca.pd_to_cassandra(combined_stats)

# # Get the list of all stock tickers from Yahoo Finance
# url = "https://finance.yahoo.com/exchanges/us/"
# response = requests.get(url)

# # Parse the HTML response and get the list of stock tickers
# soup = BeautifulSoup(response.content, "html.parser")
# tickers = soup.find_all("td", class_="yfnc_tabledata1")
# print(tickers)
# # Create a DataFrame of the stock tickers
# df = pd.DataFrame(tickers, columns=["Ticker"])
# df.head()
# Connect to AstraDB
# from datastax.astra.client import Session

# session = Session.builder() \
#     .with_contact_points("astra-serverless-us-central1.datastax.com") \
#     .with_keyspace("my_keyspace") \
#     .with_username("my_username") \
#     .with_password("my_password") \
#     .build()

# # Save the DataFrame to AstraDB
# df.to_cassandra("us_stock_tickers", session=session)

# # Close the session
# session.close()