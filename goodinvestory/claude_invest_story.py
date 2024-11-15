import yfinance as yf
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import ast
import json
from variables import ANTHROPIC_API_KEY


def get_article_text(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        article_text = ' '.join([p.get_text() for p in soup.find_all('p')])
        return article_text
    except:
        return "Error retrieving article text."

def get_stock_data(ticker, years):
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=years*365)

    stock = yf.Ticker(ticker)

    # Retrieve historical price data
    hist_data = stock.history(start=start_date, end=end_date)

    # Retrieve balance sheet
    balance_sheet = stock.balance_sheet

    # Retrieve financial statements
    financials = stock.financials

    # Retrieve news articles
    news = stock.news

    return hist_data, balance_sheet, financials, news

def get_claude_comps_analysis(ticker, hist_data, balance_sheet, financials, news):
    system_prompt = f"You are a financial analyst assistant. Analyze the given data for {ticker} and suggest a few comparable companies to consider. Do so in a Python-parseable list."

    news = ""

    for article in news:
        article_text = get_article_text(article['link'])
        news = news + f"\n\n---\n\nTitle: {article['title']}\nText: {article_text}"

    messages = [
        {"role": "user", "content": f"Historical price data:\n{hist_data.tail().to_string()}\n\nBalance Sheet:\n{balance_sheet.to_string()}\n\nFinancial Statements:\n{financials.to_string()}\n\nNews articles:\n{news.strip()}\n\n----\n\nNow, suggest a few comparable companies to consider, in a Python-parseable list. Return nothing but the list. Make sure the companies are in the form of their tickers."},
    ]

    response_text = access_claude(system_prompt, messages)

    return ast.literal_eval(response_text)

# def compare_companies(main_ticker, main_data, comp_ticker, comp_data):
#     system_prompt = f"You are a financial analyst assistant. Compare the data of {main_ticker} against {comp_ticker} and provide a detailed comparison, like a world-class analyst would. Be measured and discerning. Truly think about the positives and negatives of each company. Be sure of your analysis. You are a skeptical investor."

#     messages = [
#         {"role": "user", "content": f"Data for {main_ticker}:\n\nHistorical price data:\n{main_data['hist_data'].tail().to_string()}\n\nBalance Sheet:\n{main_data['balance_sheet'].to_string()}\n\nFinancial Statements:\n{main_data['financials'].to_string()}\n\n----\n\nData for {comp_ticker}:\n\nHistorical price data:\n{comp_data['hist_data'].tail().to_string()}\n\nBalance Sheet:\n{comp_data['balance_sheet'].to_string()}\n\nFinancial Statements:\n{comp_data['financials'].to_string()}\n\n----\n\nNow, provide a detailed comparison of {main_ticker} against {comp_ticker}. Explain your thinking very clearly."},
#     ]

#     return access_claude(system_prompt, messages)

def get_sentiment_analysis(ticker, news):
    system_prompt = f"You are a sentiment analysis assistant. Analyze the sentiment of the given news articles for {ticker} and provide a summary of the overall sentiment and any notable changes over time. Be measured and discerning. You are a skeptical investor."

    news_text = ""
    for article in news:
        article_text = get_article_text(article['link'])
        timestamp = datetime.fromtimestamp(article['providerPublishTime']).strftime("%Y-%m-%d")
        news_text += f"\n\n---\n\nDate: {timestamp}\nTitle: {article['title']}\nText: {article_text}"

    messages = [
        {"role": "user", "content": f"News articles for {ticker}:\n{news_text}\n\n----\n\nProvide a summary of the overall sentiment and any notable changes over time."},
    ]

    return access_claude(system_prompt, messages)

def get_analyst_ratings(ticker):
    stock = yf.Ticker(ticker)
    recommendations = stock.recommendations
    if recommendations is None or recommendations.empty:
        return "No analyst ratings available."

    latest_rating = recommendations.iloc[-1]

    firm = latest_rating.get('Firm', 'N/A')
    to_grade = latest_rating.get('To Grade', 'N/A')
    action = latest_rating.get('Action', 'N/A')

    rating_summary = f"Latest analyst rating for {ticker}:\nFirm: {firm}\nTo Grade: {to_grade}\nAction: {action}"

    return rating_summary

def get_industry_analysis(ticker):

    ### update to use search to find recent data!!

    stock = yf.Ticker(ticker)
    industry = stock.info['industry']
    sector = stock.info['sector']

    system_prompt = f"You are an industry analysis assistant. Provide an analysis of the {industry} industry and {sector} sector, including trends, growth prospects, regulatory changes, and competitive landscape. Be measured and discerning. Truly think about the positives and negatives of the stock. Be sure of your analysis. You are a skeptical investor."

    messages = [
        {"role": "user", "content": f"Provide an analysis of the {industry} industry and {sector} sector."},
    ]

    return access_claude(system_prompt, messages)


def get_final_analysis(ticker, comparisons, sentiment_analysis, analyst_ratings, industry_analysis):
    system_prompt = f"You are a financial analyst providing a final investment recommendation for {ticker} based on the given data and analyses. Be measured and discerning. Truly think about the positives and negatives of the stock. Be sure of your analysis. You are a skeptical investor."

    messages = [
        {"role": "user", "content": f"Ticker: {ticker}\n\nComparative Analysis:\n{json.dumps(comparisons, indent=2)}\n\nSentiment Analysis:\n{sentiment_analysis}\n\nAnalyst Ratings:\n{analyst_ratings}\n\nIndustry Analysis:\n{industry_analysis}\n\nBased on the provided data and analyses, please provide a comprehensive investment analysis and recommendation for {ticker}. Consider the company's financial strength, growth prospects, competitive position, and potential risks. Provide a clear and concise recommendation on whether to buy, hold, or sell the stock, along with supporting rationale."},
    ]

    return access_claude(system_prompt, messages)

def generate_ticker_ideas():
    system_prompt = f"You are a financial analyst assistant. Generate a list of 3 ticker symbols for major companies in the US, as a Python-parseable list."

    messages = [
        {"role": "user", "content": f"Please provide a list of 3 ticker symbols for major companies in the US as a Python-parseable list. Only respond with the list, no other text."},
    ]

    ticker_list = ast.literal_eval(access_claude(system_prompt, messages))
    return [ticker.strip() for ticker in ticker_list]

def get_current_price(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period='1d', interval='1m')
    return data['Close'][-1]

def rank_companies(industry, analyses, prices):
    system_prompt = f"You are a financial analyst providing a ranking of companies in the {industry} industry based on their investment potential. Be discerning and sharp. Truly think about whether a stock is valuable or not. You are a skeptical investor."



    analysis_text = "\n\n".join(
        f"Ticker: {ticker}\nCurrent Price: {prices.get(ticker, 'N/A')}\nAnalysis:\n{analysis}"
        for ticker, analysis in analyses.items()
    )

    messages = [
        {"role": "user", "content": f"Industry: {industry}\n\nCompany Analyses:\n{analysis_text}\n\nBased on the provided analyses, please rank the companies from most attractive to least attractive for investment. Provide a brief rationale for your ranking. In each rationale, include the current price (if available) and a price target."},
    ]

    return access_claude(system_prompt, messages)

def access_claude(system_prompt, messages):

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    data = {
        "model": 'claude-3-opus-20240229',
        "max_tokens": 3000,
        "temperature": 0.5,
        "system": system_prompt,
        "messages": messages,
    }
    response = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=data)
    response_text = response.json()['content'][0]['text']

    return response_text

# User input
# industry = input("Enter the industry to analyze: ")
years = 1 # int(input("Enter the number of years for analysis: "))

# Generate ticker ideas for the industry
tickers = generate_ticker_ideas()
print(f"\nTickers being analyzed:")
print(", ".join(tickers))

# Perform analysis for each company
analyses = {}
prices = {}
for ticker in tickers:
    try:
        print(f"\nAnalyzing {ticker}...")
        hist_data, balance_sheet, financials, news = get_stock_data(ticker, years)
        main_data = {
            'hist_data': hist_data,
            'balance_sheet': balance_sheet,
            'financials': financials,
            'news': news
        }
        sentiment_analysis = get_sentiment_analysis(ticker, news)
        analyst_ratings = get_analyst_ratings(ticker)
        industry_analysis = get_industry_analysis(ticker)
        final_analysis = get_final_analysis(ticker, {}, sentiment_analysis, analyst_ratings, industry_analysis)
        analyses[ticker] = final_analysis
        # prices[ticker] = get_current_price(ticker)
    except:
        pass
print(analyses)