import yfinance as yf

# Define your list of NSE tickers with ".NS" suffix
tickers = ["AMARAJ.NS", "CADHEA.NS", "DRREDD.NS", "EXIIND.NS", "FEDBAN.NS",
          "GOLDEX.NS", "HCLTEC.NS", "HDFC.NS", "IDFBAN.NS", "IDFC.NS",
          "INFTEC.NS", "ITC.NS", "KARBAN.NS", "KOTN20.NS", "MANAFI.NS",
          "MOTSUM.NS", "MUTCAP.NS", "NATPHA.NS", "NIFTY.NS", "RAMSYS.NS",
          "RENV20.NS", "SOUBAN.NS", "STABAN.NS", "TATACHEM.NS", "TATAMOTORS.NS",
          "TATasteel.NS", "TATATELE.NS", "TCS.NS", "TECMAHENDRA.NS", "TATAMTRDVR.NS",
          "WIPRO.NS"]

# Download stock information for all tickers
data = yf.download(tickers, period="1d", interval="1m")  # Download 1 minute data

# Extract the current closing price for each ticker (assuming the latest data point is the closing price)
prices = {}
for ticker in tickers:
  try:
    # Access the 'Close' price for the last data point (assuming it's the latest)
    price = data[ticker]["Close"][-1]
    prices[ticker] = price
  except (KeyError, IndexError):
    prices[ticker] = None  # Handle cases where data is unavailable

# Print the ticker and its current price
for ticker, price in prices.items():
  if price is not None:
    print(f"{ticker}: {price}")
  else:
    print(f"{ticker}: Price unavailable")
