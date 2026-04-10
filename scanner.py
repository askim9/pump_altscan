python3 -c "
import requests
r = requests.get('https://api.bitget.com/api/v2/mix/market/tickers', params={'productType':'USDT-FUTURES'})
items = r.json()['data']
for item in items:
    if item['symbol'] in ['DASHUSDT','CHILLGUYUSDT']:
        print('=== ', item['symbol'], '===')
        for k, v in item.items():
            print(f'  {k}: {v}')
        print()
"
