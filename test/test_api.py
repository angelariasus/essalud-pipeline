import requests
import json

def test_endpoint(endpoint, params):
    r = requests.get(f'https://contratacionesabiertas.oece.gob.pe/api/v1/{endpoint}', params=params)
    data = r.json()
    records = data.get('records', [])
    if not records:
        print(f'{params} -> No records returned.')
        return
    buyer_name = records[0].get('compiledRelease', {}).get('buyer', {}).get('name')
    print(f'{params} -> First Record Buyer Name: {buyer_name}')

test_endpoint('records', {'buyer.id': '20131257750', 'size': 1})
test_endpoint('records', {'buyer.identifier.id': '20131257750', 'size': 1})
test_endpoint('records', {'q': '20131257750', 'size': 1})
test_endpoint('records', {'search': '20131257750', 'size': 1})
