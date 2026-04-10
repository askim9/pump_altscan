import requests

r = requests.get(
    'https://api.bitget.com/api/v2/mix/market/tickers',
    params={'productType': 'USDT-FUTURES'}
)
items = r.json()['data']

# Tampilkan semua field dari beberapa coin target
targets = ['DASHUSDT', 'CHILLGUYUSDT', 'RAVEUSDT', 'BTCUSDT']
for item in items:
    if item['symbol'] in targets:
        print(f"\n=== {item['symbol']} ===")
        for k, v in item.items():
            print(f"  {k}: {v}")
