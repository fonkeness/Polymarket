import requests

def get_markets():
    url = "https://api.polymarket.com/markets"
    response = requests.get(url)
    data = response.json()
    return data
markets = get_markets()
print(len(markets))
print(markets[0])
