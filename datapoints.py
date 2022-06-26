import sqlite3
from datetime import datetime, timedelta
import requests
from flask import Flask, request

app = Flask(__name__, static_url_path='/static')

def check_liquidity_by_subgraph(address):
    pools = []
    data = """{"query":"{pools(orderBy: createdTimestamp, orderDirection: desc) { id baseTokenLiquidity }}","variables":null,"operationName":null}"""
    response = requests.post('https://v4.subgraph.polygon.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph', data=data).json()

    for pool in response['data']['pools']:
        if pool['id'].split('-')[0] == address:
            return pool['baseTokenLiquidity']

@app.route('/liquidity_volume', methods=['GET'])
def liquidity_volume():
    address = request.args.get('address', default = 1, type = str).lower()
    now = datetime.now()
    
    liquidity_12h = 0
    liquidity_24h = 0
    liquidity_3d = 0
    liquidity_7d = 0
    liquidity_30d = 0
    liquidity_90d = 0

    
    conn = sqlite3.connect('ocean_data.db')
    cursor = conn.cursor()

    try:
        query = "SELECT * FROM '"+address+"_txs'"
        txs = cursor.execute(query)
    except:
        return {'error': 'No pool found with this address'}

    for tx in txs:

        if tx[0] == 'liquidity_in':
            tx_time = datetime.fromisoformat(tx[-1].replace('T',' ').replace('Z',''))

            if now >= tx_time >= now-timedelta(hours=12):
                liquidity_12h += tx[1]                
            
            if now >= tx_time >= now-timedelta(hours=24):
                liquidity_24h += tx[1]

            if now >= tx_time >= now-timedelta(days=3):
                liquidity_3d += tx[1]

            if now >= tx_time >= now-timedelta(days=7):
                liquidity_7d += tx[1]
           

            if now >= tx_time >= now-timedelta(days=30):
                liquidity_30d += tx[1]

            
            if now >= tx_time >= now-timedelta(days=90):
                liquidity_90d += tx[1]


        if tx[0] == 'liquidity_out':
            tx_time = datetime.fromisoformat(tx[-1].replace('T',' ').replace('Z',''))

            if now >= tx_time >= now-timedelta(hours=12):
                liquidity_12h += tx[2]                
            
            if now >= tx_time >= now-timedelta(hours=24):
                liquidity_24h += tx[2]         

            if now >= tx_time >= now-timedelta(days=3):
                liquidity_3d += tx[2]    

            if now >= tx_time >= now-timedelta(days=7):
                liquidity_7d += tx[2]               

            if now >= tx_time >= now-timedelta(days=30):
                liquidity_30d += tx[2]
            
            if now >= tx_time >= now-timedelta(days=90):
                liquidity_90d += tx[2]
            
    results = {
        'pool_address': address,
        'liquidity_volume_12h': liquidity_12h,
        'liquidity_volume_24h': liquidity_24h,
        'liquidity_volume_3d': liquidity_3d,
        'liquidity_volume_7d': liquidity_7d,
        'liquidity_volume_30d': liquidity_30d,
        'liquidity_volume_90d': liquidity_90d
        }
    
    return results
               
@app.route('/txs_volume', methods=['GET'])
def txs_volume():
    address = request.args.get('address', default = 1, type = str).lower()
    now = datetime.now()
    
    liquidity_txs_12h = 0
    liquidity_txs_24h = 0
    liquidity_txs_3d = 0
    liquidity_txs_7d = 0
    liquidity_txs_30d = 0
    liquidity_txs_90d = 0

    
    conn = sqlite3.connect('ocean_data.db')
    cursor = conn.cursor()

    try:
        query = "SELECT * FROM '"+address+"_txs'"
        txs = cursor.execute(query)
    except:
        return {'error': 'No pool found with this address'}

    for tx in txs:
        if 'liquidity' in tx[0]:
            tx_time = datetime.fromisoformat(tx[-1].replace('T',' ').replace('Z',''))

            if now >= tx_time >= now-timedelta(hours=12):
                liquidity_txs_12h += 1                
            
            if now >= tx_time >= now-timedelta(hours=24):
                liquidity_txs_24h += 1

            if now >= tx_time >= now-timedelta(days=3):
                liquidity_txs_3d += 1

            if now >= tx_time >= now-timedelta(days=7):
                liquidity_txs_7d += 1        

            if now >= tx_time >= now-timedelta(days=30):
                liquidity_txs_30d += 1
        
            if now >= tx_time >= now-timedelta(days=90):
                liquidity_txs_90d += 1


    results = {
        'pool_address': address,
        'liquidity_txs_volume_12h': liquidity_txs_12h,
        'liquidity_txs_volume_24h': liquidity_txs_24h,
        'liquidity_txs_volume_3d': liquidity_txs_3d,
        'liquidity_txs_volume_7d': liquidity_txs_7d,
        'liquidity_txs_volume_30d': liquidity_txs_30d,
        'liquidity_txs_volume_90d': liquidity_txs_90d
        }
    
    return results

@app.route('/average_liquidity_provided', methods=['GET'])
def average_liquidity_provided():
    address = request.args.get('address', None).lower()
    providers = []
    
    conn = sqlite3.connect('ocean_data.db')
    cursor = conn.cursor()

    query = "SELECT * FROM '{0}_txs'".format(address)
    txs = cursor.execute(query).fetchall()
    total_liquidity = check_liquidity_by_subgraph(address)

    for tx in txs:
        if 'liquidity_in' in tx[0]:
            if tx[5] not in providers:
                providers.append(tx[5])

        if 'liquidity_out' in tx[0]:
            if tx[5] in providers:
                providers.remove(tx[5])
                
    results = {
        'average_liquidity_provided': float(float(total_liquidity)/len(providers)),
        }
    return results
    
if __name__ == "__main__":
    app.run(debug=True) 

