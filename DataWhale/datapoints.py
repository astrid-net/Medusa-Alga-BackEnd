import sqlite3
from datetime import datetime, timedelta
import requests
from flask import Flask, request
import statistics

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
    address = request.args.get('address', default = 1, type = str)
    now = datetime.now()
    
    liquidity_12h = 0
    liquidity_24h = 0
    liquidity_3d = 0
    liquidity_7d = 0
    liquidity_30d = 0
    liquidity_90d = 0

    
    conn = sqlite3.connect('Data/ocean_data.db')
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
    address = request.args.get('address', default = 1, type = str)
    now = datetime.now()
    
    liquidity_txs_12h = 0
    liquidity_txs_24h = 0
    liquidity_txs_3d = 0
    liquidity_txs_7d = 0
    liquidity_txs_30d = 0
    liquidity_txs_90d = 0

    
    conn = sqlite3.connect('Data/ocean_data.db')
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
    address = request.args.get('address', None)
    providers = []
    
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

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

@app.route('/token_swapped', methods=['GET'])
def token_swapped():
    tot_datatoken_swapped=0
    tot_basetoken_swapped=0
    
    address = request.args.get('address', None)
    providers = []
    
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    for tx in txs:
        if 'swap_bt_in' in tx[0]:
            tot_basetoken_swapped+=tx[1]
        if 'swap_bt_out' in tx[0]:
            tot_datatoken_swapped+=tx[3]
    result = {
        'total_bt_swapped_for_dt': tot_basetoken_swapped,
        'total_dt_swapped_for_bt': tot_datatoken_swapped,
        }
    return result

@app.route('/swap_unswap', methods=['GET'])
def swap_unswap():
    swap_24h = 0
    swap_7d = 0
    swap_30d = 0
    swap_90d = 0

    unswap_24h = 0
    unswap_7d = 0
    unswap_30d = 0
    unswap_90d = 0
    
    address = request.args.get('address', None)
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}
    
    now = datetime.now()

    for tx in txs:
        if 'swap_bt_in' in tx[0]:
            tx_time = datetime.fromisoformat(tx[-1].replace('T',' ').replace('Z',''))

            if now>=tx_time>=now-timedelta(hours=24):
                swap_24h+=1
            if now>=tx_time>=now-timedelta(days=7):
                swap_7d+=1            
            if now>=tx_time>=now-timedelta(days=30):
                swap_30d+=1
            if now>=tx_time>=now-timedelta(days=90):
                swap_90d+=1

        if 'swap_bt_out' in tx[0]:
            tx_time = datetime.fromisoformat(tx[-1].replace('T',' ').replace('Z',''))

            if now>=tx_time>=now-timedelta(hours=24):
                unswap_24h+=1
            if now>=tx_time>=now-timedelta(days=7):
                unswap_7d+=1            
            if now>=tx_time>=now-timedelta(days=30):
                unswap_30d+=1
            if now>=tx_time>=now-timedelta(days=90):
                unswap_90d+=1

    results = {
        'total_swaps': {
            '24h': swap_24h,
            '7d': swap_7d,
            '30d': swap_30d,
            '90d': swap_90d,
            },
        'total_unswaps': {
            '24h': unswap_24h,
            '7d': unswap_7d,
            '30d': unswap_30d,
            '90d': unswap_90d,
            },        
        }
    
    return results

@app.route('/liquidity_history', methods=['GET'])                                     
def liquidity_history():
    end_date = datetime.now()
    address = request.args.get('address', None)
    date_filter = request.args.get('datetime_filter', None)
    date_start = None

    if date_filter == 'day':
        date_start = end_date-timedelta(days=1)
    if date_filter == 'week':
        date_start = end_date-timedelta(days=7)
    if date_filter == 'month':
        date_start = end_date-timedelta(days=30)        
    if date_filter == 'three_month':
        date_start = end_date-timedelta(days=90)
    if date_filter == 'six_month':
        date_start = end_date-timedelta(days=180)
        
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    liquidity = []
    dates = []

    
    try:
        query = "SELECT * FROM datasets_list WHERE pool_address = '{0}'".format(address)
        starting_liquidity = cursor.execute(query).fetchall()[0][7]
        db_date_start = cursor.execute(query).fetchall()[0][6]

        if db_date_start > date_start or date_start == None:
            date_start = db_date_start
            
    except Exception as e:
        return {'error': 'No pool found with this address','e':str(e)}

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    start_date = datetime.fromisoformat(date_start)
    liquidity.append(starting_liquidity)
    dates.append(start_date.replace(hour = 0, minute=0, second=0, microsecond=0))

    for n in range(int((end_date-start_date).days)):
        
        #DIVIDE EACH DAY IN FOUR TIME_FRAMES. FOR EACH TIMEFRAME WE CREATE AN ARRAY
        first_half_of_day = (start_date+timedelta(n)).replace(hour = 0, minute=0, second=0, microsecond=0)
        first_half_day_txs = []
        first_half_day_liquidity = liquidity[-1]
        
        second_half_of_day = first_half_of_day+timedelta(hours=6)
        second_half_day_txs = []
        second_half_day_liquidity = 0
        
        third_half_of_day = second_half_of_day+timedelta(hours=6)
        third_half_day_txs = []
        third_half_day_liquidity = 0
        
        four_half_of_day = third_half_of_day+timedelta(hours=6)
        four_half_day_txs = []
        four_half_day_liquidity = 0

        for tx in txs:
            if 'swap' in tx[0]:
                continue
            
            tx_time = datetime.fromisoformat(tx[-1].replace('T', ' ').replace('Z',''))
            
            if tx_time<first_half_of_day:
                continue          
            if tx_time>four_half_of_day:
                break

            if tx[0] == 'liquidity_in':
            
                if first_half_of_day <= tx_time < second_half_of_day:
                    first_half_day_txs.append(tx[1])
                    continue
                    
                if second_half_of_day <= tx_time < third_half_of_day:
                    second_half_day_txs.append(tx[1])
                    continue

                if third_half_of_day <= tx_time < four_half_of_day:
                    third_half_day_txs.append(tx[1])
                    continue

                if four_half_of_day <= tx_time < four_half_of_day+timedelta(hours=6):
                    four_half_day_txs.append(tx[1])
                    continue
                
            if tx[0] == 'liquidity_out':
            
                if first_half_of_day <= tx_time < second_half_of_day:
                    first_half_day_txs.append(-tx[2])
                    continue
                    
                if second_half_of_day <= tx_time < third_half_of_day:
                    second_half_day_txs.append(-tx[2])
                    continue

                if third_half_of_day <= tx_time < four_half_of_day:
                    third_half_day_txs.append(-tx[2])
                    continue

                if four_half_of_day <= tx_time < four_half_of_day+timedelta(hours=6):
                    four_half_day_txs.append(-tx[2])
                    continue
        first_half_day_liquidity = first_half_day_liquidity + sum(first_half_day_txs)
        second_half_day_liquidity = first_half_day_liquidity + sum(second_half_day_txs)
        third_half_day_liquidity = second_half_day_liquidity + sum(third_half_day_txs)
        four_half_day_liquidity = third_half_day_liquidity + sum(four_half_day_txs)

        liquidity.extend([first_half_day_liquidity, second_half_day_liquidity, third_half_day_liquidity, four_half_day_liquidity])
        dates.extend([first_half_of_day, second_half_of_day, third_half_of_day, four_half_of_day])

            
    result = {
        'liquidity': liquidity[1:-1],
        'datetime': dates[1:-1],
        }

    return result       

@app.route('/price_history', methods=['GET'])                                     
def price_history():
    
    end_date = datetime.now()
    address = request.args.get('address', None)
    date_filter = request.args.get('datetime_filter', None)
    date_start = None

    price = []
    dates = []

    if date_filter == 'day':
        date_start = end_date-timedelta(days=1)
    if date_filter == 'week':
        date_start = end_date-timedelta(days=7)
    if date_filter == 'month':
        date_start = end_date-timedelta(days=30)        
    if date_filter == 'three_month':
        date_start = end_date-timedelta(days=90)
    if date_filter == 'six_month':
        date_start = end_date-timedelta(days=180)
        
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    txs = []

    
    try:
        query = "SELECT * FROM datasets_list WHERE pool_address = '{0}'".format(address)
        starting_price = cursor.execute(query).fetchall()[0][9]
        db_date_start = datetime.fromisoformat(cursor.execute(query).fetchall()[0][6])

        if db_date_start > date_start or date_start == None:
            date_start = db_date_start

    except Exception as e:
        return {'error': 'No pool found with this address','e':str(e)}
    
    price.append(starting_price)
    dates.append(date_start)

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs_all = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    for tx in txs_all:
        if tx[0] == 'swap_bt_in':
            txs.extend([[tx[-1], tx[1]/tx[4]]])
        if tx[0] == 'swap_bt_out':
            txs.extend([[tx[-1], tx[2]/tx[3]]])

    #IF ALREADY STRING CONVERT IT
    try:      
        start_date = datetime.fromisoformat(date_start)
    except:
        start_date = date_start

    for n in range(int((end_date-start_date).days)):

        #DIVIDE EACH DAY IN FOUR TIME_FRAMES. FOR EACH TIMEFRAME WE CREATE AN ARRAY
        first_half_of_day = (start_date+timedelta(n)).replace(hour = 0, minute=0, second=0, microsecond=0)
        first_half_day_txs = []
        
        second_half_of_day = first_half_of_day+timedelta(hours=6)
        second_half_day_txs = []
        
        third_half_of_day = second_half_of_day+timedelta(hours=6)
        third_half_day_txs = []
        
        four_half_of_day = third_half_of_day+timedelta(hours=6)
        four_half_day_txs = []
        

        #FETCH TRANSACTIONS. IF THEY ARE BEFORE THE FIRST TIME-FRAME WE SKIP. IF AFTER THE LAST TRANSACTIONBREAK THE LOOP.
        for tx in txs:
            tx_time = datetime.fromisoformat(tx[0].replace('T', ' ').replace('Z','')).replace(minute=0, second=0, microsecond=0)
            
            if tx_time<first_half_of_day:
                continue
            
            if tx_time>four_half_of_day+timedelta(hours=6):
                break

            #APPEND VALUE OF TX ON EACH TIMEFRAME IF BELONGS TO IT.
            if first_half_of_day <= tx_time < second_half_of_day:
                first_half_day_txs.append(tx[1])
                continue
                
            if second_half_of_day <= tx_time < third_half_of_day:
                second_half_day_txs.append(tx[1])
                continue

            if third_half_of_day <= tx_time < four_half_of_day:
                third_half_day_txs.append(tx[1])
                continue

            if four_half_of_day <= tx_time < four_half_of_day+timedelta(hours=6):
                four_half_day_txs.append(tx[1])
                continue


        #IF TX IN TIMEFRAMES ARE ZERO THEN APPEND THE SAME VALUE OF LAST TRANSACTION.
        #IF NOT NULL, CALCULATE THE MEAN OF PRICE PAID. 
        if len(first_half_day_txs) == 0:
            price.append(price[-1])
            
        if len(first_half_day_txs) != 0:    
            price.append(sum(first_half_day_txs)/len(first_half_day_txs))
            
        dates.append(first_half_of_day)
            

        if len(second_half_day_txs) == 0:
            price.append(price[-1])
            
        if len(second_half_day_txs) != 0:    
            price.append(sum(second_half_day_txs)/len(second_half_day_txs))
            
        dates.append(second_half_of_day)


        if len(third_half_day_txs) == 0:
            price.append(price[-1])
            
        if len(third_half_day_txs) != 0:    
            price.append(sum(third_half_day_txs)/len(third_half_day_txs))
            
        dates.append(third_half_of_day)


        if len(four_half_day_txs) == 0:
            price.append(price[-1])
            
        if len(four_half_day_txs) != 0:    
            price.append(sum(four_half_day_txs)/len(four_half_day_txs))

        dates.append(four_half_of_day)
            
    result = {
        'price': price[1:-1],
        'datetime': dates[1:-1],
        }
    
    for p,d in zip(result['price'], result['datetime']):
        print(d, p)
        
    return result        
                 
@app.route('/price_stats', methods=['GET'])                                    
def price_stats():
    address = request.args.get('address', None)
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    price = []

    
    try:
        query = "SELECT * FROM datasets_list WHERE pool_address = '{0}'".format(address)
        starting_price = cursor.execute(query).fetchall()[0][9]
    except Exception as e:
        return {'error': 'No pool found with this address','e':str(e)}
    
    price.append(starting_price)

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    for tx in txs_all:
        if tx[0] == 'swap_bt_in':
            price.append(tx[1]/tx[4])
        if tx[0] == 'swap_bt_out':
            price.extend(tx[2]/tx[3])

    result = {
        'max_price': max(price),
        'min_price': min(price),
        }
    
@app.route('/liquidity_stats', methods=['GET'])                                    
def liquidity_stats():
    end_date = datetime.now()
    address = request.args.get('address', None)
       
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    liquidity = []
    dates = []

    
    try:
        query = "SELECT * FROM datasets_list WHERE pool_address = '{0}'".format(address)
        starting_liquidity = cursor.execute(query).fetchall()[0][7]
        date_start = cursor.execute(query).fetchall()[0][6]
            
    except Exception as e:
        return {'error': 'No pool found with this address','e':str(e)}

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    start_date = datetime.fromisoformat(date_start)
    liquidity.append(starting_liquidity)
    dates.append(start_date.replace(hour = 0, minute=0, second=0, microsecond=0))

    for n in range(int((end_date-start_date).days)):
        
        #DIVIDE EACH DAY IN FOUR TIME_FRAMES. FOR EACH TIMEFRAME WE CREATE AN ARRAY
        first_half_of_day = (start_date+timedelta(n)).replace(hour = 0, minute=0, second=0, microsecond=0)
        first_half_day_txs = []
        first_half_day_liquidity = liquidity[-1]
        
        second_half_of_day = first_half_of_day+timedelta(hours=6)
        second_half_day_txs = []
        second_half_day_liquidity = 0
        
        third_half_of_day = second_half_of_day+timedelta(hours=6)
        third_half_day_txs = []
        third_half_day_liquidity = 0
        
        four_half_of_day = third_half_of_day+timedelta(hours=6)
        four_half_day_txs = []
        four_half_day_liquidity = 0

        for tx in txs:
            if 'swap' in tx[0]:
                continue
            
            tx_time = datetime.fromisoformat(tx[-1].replace('T', ' ').replace('Z',''))
            
            if tx_time<first_half_of_day:
                continue          
            if tx_time>four_half_of_day:
                break

            if tx[0] == 'liquidity_in':
            
                if first_half_of_day <= tx_time < second_half_of_day:
                    first_half_day_txs.append(tx[1])
                    continue
                    
                if second_half_of_day <= tx_time < third_half_of_day:
                    second_half_day_txs.append(tx[1])
                    continue

                if third_half_of_day <= tx_time < four_half_of_day:
                    third_half_day_txs.append(tx[1])
                    continue

                if four_half_of_day <= tx_time < four_half_of_day+timedelta(hours=6):
                    four_half_day_txs.append(tx[1])
                    continue
                
            if tx[0] == 'liquidity_out':
            
                if first_half_of_day <= tx_time < second_half_of_day:
                    first_half_day_txs.append(-tx[2])
                    continue
                    
                if second_half_of_day <= tx_time < third_half_of_day:
                    second_half_day_txs.append(-tx[2])
                    continue

                if third_half_of_day <= tx_time < four_half_of_day:
                    third_half_day_txs.append(-tx[2])
                    continue

                if four_half_of_day <= tx_time < four_half_of_day+timedelta(hours=6):
                    four_half_day_txs.append(-tx[2])
                    continue
        first_half_day_liquidity = first_half_day_liquidity + sum(first_half_day_txs)
        second_half_day_liquidity = first_half_day_liquidity + sum(second_half_day_txs)
        third_half_day_liquidity = second_half_day_liquidity + sum(third_half_day_txs)
        four_half_day_liquidity = third_half_day_liquidity + sum(four_half_day_txs)

        liquidity.extend([first_half_day_liquidity, second_half_day_liquidity, third_half_day_liquidity, four_half_day_liquidity])

    result = {
        'max_liquidity': max(liquidity),
        'min_liquidity': min(liquidity)
        }
    
    return result

@app.route('/total_consumers', methods=['GET'])                                    
def consumers_number():
    address = request.args.get('address', None)
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()

    try:
        query = "SELECT * FROM '{0}_consumers'".format(address)
        n_consumers = len(cursor.execute(query).fetchall())
    except:
        return {'error': 'No pool found with this address'}

    result = {
        'total_consumers': n_consumers
        }
    return result


@app.route('/consumers_price', methods=['GET'])                                    
def consumers_price():
    address = request.args.get('address', None)
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    price = []
    date = []
    price_consumer = []


    try:
        query = "SELECT * FROM '{0}_consumers'".format(address)
        consumers = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    
    try:
        query = "SELECT * FROM datasets_list WHERE pool_address = '{0}'".format(address)
        starting_price = cursor.execute(query).fetchall()[0][9]
        starting_date = cursor.execute(query).fetchall()[0][6]
    except Exception as e:
        return {'error': 'No pool found with this address','e':str(e)}
    
    price.append(starting_price)
    date.append(starting_date)

    try:
        query = "SELECT * FROM '{0}_txs'".format(address)
        txs = cursor.execute(query).fetchall()
    except:
        return {'error': 'No pool found with this address'}

    for tx in txs:
        if tx[0] == 'swap_bt_in':
            price.append(tx[1]/tx[4])
            date.append(tx[-1])
        if tx[0] == 'swap_bt_out':
            price.append(tx[2]/tx[3])
            date.append(tx[-1])

    for c in consumers:
        c = datetime.fromisoformat(c[0])

        for p, i in zip(price, range(len(date))):
            try:
                if datetime.fromisoformat(date[i].replace('T',' ').replace('Z',''))<=c<=datetime.fromisoformat(date[i+1].replace('T',' ').replace('Z','')):
                    price_consumer.append(p)
            except Exception as e:
                print(e)
                continue
    if len(price_consumer)==0:
        result = {
            'mean': 'no consumer found for this pool',
            'median': 'no consumer found for this pool'
            }
    else:
        result = {
            'mean_price': statistics.mean(price_consumer),
            'median_price': statistics.median(price_consumer)
            }
        
    return result

    

if __name__ == "__main__":
   app.run(debug=True) 

