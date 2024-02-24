import requests
from bs4 import BeautifulSoup
from models import OptionData
from database import save_option_data

def scrape_options_data():
    # URL to scrape
    url = "https://finance.yahoo.com/quote/SPY/options"
    
    # Send HTTP request to the URL
    response = requests.get(url)
    
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract options data
    # This is a simplified example; you'll need to locate the actual data within the HTML
    options = soup.find_all('div', class_='option-data-class')
    for option in options:
        # Extract data points (e.g., strike price, expiration date)
        strike_price = option.find('span', class_='strike-price').text
        expiration_date = option.find('span', class_='expiration-date').text
        
        # Create an OptionData object (defined in models.py)
        option_data = OptionData(strike_price=strike_price, expiration_date=expiration_date)
        
        # Save the data to the database
        save_option_data(option_data)
