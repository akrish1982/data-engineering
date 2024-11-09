import requests
from bs4 import BeautifulSoup
# from variables import ANTHROPIC_API_KEY

def get_article_text(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        article_text = ' '.join([p.get_text() for p in soup.find_all('p')])
        return article_text
    except:
        return "Error retrieving article text."
    
url = "https://11c9e6b4.iexpertify.pages.dev/learn/netezza-overview/"

print(get_article_text(url))