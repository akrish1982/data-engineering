import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

def next_n_fridays(n=4):
    today = datetime.now()
    fridays = []
    for _ in range(n * 7):  # Search within the next n*7 days
        if today.weekday() == 4:  # Friday
            fridays.append(today)
            if len(fridays) == n:
                break
        today += timedelta(days=1)
    return [friday.strftime('%Y-%m-%d') for friday in fridays]

def fetch_options_data(date):
    url = f"https://finance.yahoo.com/quote/SPY/options?date={date}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    # This part depends on the structure of the page and may require adjustment
    # Below is a conceptual approach
    tables = soup.find_all('table')
    if not tables:
        print(f"No data found for {date}")
        return pd.DataFrame()
    dfs = pd.read_html(str(tables[0])) + pd.read_html(str(tables[1]))  # Assumes 2 tables for calls & puts
    for df in dfs:
        df['Expiry'] = datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d')
    return pd.concat(dfs)

def main():
    fridays = next_n_fridays(4)
    all_data = []
    for friday in fridays:
        # Convert Friday date to timestamp required by Yahoo Finance URL
        timestamp = int(pd.Timestamp(friday).timestamp())
        data = fetch_options_data(timestamp)
        all_data.append(data)
    final_df = pd.concat(all_data)
    # Write to CSV
    final_df.to_csv('spy_options_data.csv', index=False)
    print("Data written to spy_options_data.csv")

if __name__ == "__main__":
    main()
