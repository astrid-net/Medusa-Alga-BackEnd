from time import sleep
import requests
import sqlite3
import web3
from web3 import Web3
import json
import ast
import re
from datetime import datetime
from web3.middleware import geth_poa_middleware

def fetch_txs(address, chain, node_url, explorer_url, abi):
    txs = []
    txs_data = []

    MORALIS_API_KEY="myH55EK7qkfufMejJTEijIZUFoav6mLIRhnRaLCvYCQViQHYJn2lavJecapDMSDW"
    headers = {'X-API-Key':f"{MORALIS_API_KEY}"}
    r = requests.get(f"https://deep-index.moralis.io/api/v2/{address}?chain={chain}", headers=headers).json()

    for tx in r['result']:
        if tx['receipt_status'] == '1':
            txs.append(tx)        

    if r['total'] < 100:
        pass
        
    if r['total'] > 100:
        total_txs = r['total']
        last_block = txs[-1]['block_number']
        n_iter = round(total_txs/100)

        for i in range(n_iter):
            MORALIS_API_KEY="myH55EK7qkfufMejJTEijIZUFoav6mLIRhnRaLCvYCQViQHYJn2lavJecapDMSDW"
            headers = {'X-API-Key':f"{MORALIS_API_KEY}"}
            r = requests.get(f"https://deep-index.moralis.io/api/v2/{address}?chain={chain}&to_block={last_block}", headers=headers).json()

            for tx in r['result']:
                if tx['receipt_status'] == '1' and tx not in txs:
                    txs.append(tx)
            last_block = txs[-1]['block_number']

    web3 = Web3(Web3.HTTPProvider(node_url))
        
    for tx in reversed(txs):
        contract = web3.eth.contract(address=Web3.toChecksumAddress(tx["to_address"]), abi=abi['result'])
        func_obj, func_params = contract.decode_function_input(tx["input"])
        txs_data.append({'hash':tx['hash'], 'block': tx['block_number'], 'object': str(func_obj), 'parameters': str(func_params), 'sender': tx['from_address'], 'datetime': tx['block_timestamp']})

    return txs_data

def decode_tx_polygon(tx_hash):
    node_url = 'https://polygon-rpc.com/'
    web3 = Web3(Web3.HTTPProvider(node_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    tx_receipt = web3.eth.getTransaction(tx_hash)
    timestamp = web3.eth.getBlock(tx_receipt['blockNumber'])['timestamp']
    return datetime.fromtimestamp(timestamp)

def decode_tx_ethereum(tx_hash):
    node_url = "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161"
    web3 = Web3(Web3.HTTPProvider(node_url))

    tx_receipt = web3.eth.getTransaction(tx_hash)
    timestamp = web3.eth.getBlock(tx_receipt['blockNumber'])['timestamp']
    return datetime.fromtimestamp(timestamp)    
   
def check_liquidity(address, basetoken_address, node_url, abi, explorer_url):
    web3 = Web3(Web3.HTTPProvider(node_url))
    pair_contract = web3.eth.contract(address=Web3.toChecksumAddress(address), abi=abi['result'])
    return pair_contract.functions.getBalance(Web3.toChecksumAddress(basetoken_address)).call()

def check_liquidity_by_subgraph(address):
    pools = []
    data = """{"query":"{pools(orderBy: createdTimestamp, orderDirection: desc) { baseTokenLiquidity }}","variables":null,"operationName":null}"""
    response = requests.post('https://v4.subgraph.polygon.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph', data=data).json()

    for pool in response['data']['pools']:
        if pool['id'].split('-')[0] == address:
            return pool['baseTokenLiquidity']


def datatoken_price(address, basetoken_address, datatoken_address, node_url, abi, explorer_url):
    web3 = Web3(Web3.HTTPProvider(node_url))
    pair_contract = web3.eth.contract(address=Web3.toChecksumAddress(address), abi=abi['result'])
    return pair_contract.functions.getSpotPrice(Web3.toChecksumAddress(basetoken_address),Web3.toChecksumAddress(datatoken_address), 40000000000000000).call()/1000000000000000000


def starting_liquidity(address, tx, network):
    block_number = str(int(tx[-1]))

    data = {
        "query": "{poolSnapshots(where:{pool:\"%s\"}, orderBy: date, block: {number:%s}){id baseTokenLiquidity datatokenLiquidity spotPrice}}" % (address,block_number),
        "operationName": None,
        "variables": None,
    }
    
    if network == 'polygon':
        url = 'https://v4.subgraph.polygon.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph'
        response = requests.post(url, data=json.dumps(data)).json()

    if network == 'ethereum':
        url = 'https://v4.subgraph.mainnet.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph'
        response = requests.post(url, data=json.dumps(data)).json()

    for pool in response['data']['poolSnapshots']:
        if pool['id'].split('-')[0] == address:
            basetoken_liquidity_start = pool['baseTokenLiquidity']
            datatoken_liquidity_start = pool['datatokenLiquidity']
            token_ratio = float(float(basetoken_liquidity_start)/float(datatoken_liquidity_start))
            datatoken_price_start = pool['spotPrice']
                
            return basetoken_liquidity_start, datatoken_liquidity_start, datatoken_price_start, token_ratio
            


def update_datasets():
    datasets = []
    
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    query = "SELECT pool_address FROM datasets_list"
    
    #datasets_already_exist = [dataset[0] for dataset in cursor.execute(query).fetchall()]
    datasets_already_exist = []
    conn.commit()
    conn.close()
    
    datasets = []
    data = """{"query":"{pools(orderBy: createdTimestamp, orderDirection: desc) {id datatoken {address createdTimestamp} liquidityProviderSwapFee publishMarketSwapFee}}","variables":null,"operationName":null}"""
    response = requests.post('https://v4.subgraph.polygon.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph', data=data).json()
    
    for pool in response['data']['pools']:
        if pool['id'] not in datasets_already_exist:
            datasets.extend([[pool['id'], pool['datatoken']['address'], 'polygon', pool['liquidityProviderSwapFee'], pool['publishMarketSwapFee'], datetime.utcfromtimestamp(pool['datatoken']['createdTimestamp'])]])

    data = """{"query":"{pools(orderBy: createdTimestamp, orderDirection: desc) {id datatoken {address createdTimestamp} liquidityProviderSwapFee publishMarketSwapFee}}","variables":null,"operationName":null}"""
    response = requests.post('https://v4.subgraph.mainnet.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph', data=data).json()

    for pool in response['data']['pools']:
        if pool['id'] not in datasets_already_exist:
            datasets.extend([[pool['id'], pool['datatoken']['address'], 'ethereum', pool['liquidityProviderSwapFee'], pool['publishMarketSwapFee'], datetime.utcfromtimestamp(pool['datatoken']['createdTimestamp'])]])

    return datasets


def extract_dataset_data(datasets):
    datasets_def = []
    
    for dataset in datasets:
        datatoken_address = dataset[1]
        address = dataset[0]
        fee = float(dataset[3])+float(dataset[3])
        network = dataset[2]
        txn_history = []
        
        
        if dataset[2] == 'ethereum':
            basetoken_address = '0x967da4048cD07aB37855c090aAF366e4ce1b9F48'
            chain = 'eth'
            node_url = "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161"
            explorer_url ="https://api.etherscan.io/api"
            API_KEY="XS54T9YEXVT2RPXBWDJHUT5D6DCSIGWF73"
            
        if dataset[2] == 'polygon':
            basetoken_address = '0x282d8efce846a88b159800bd4130ad77443fa1a1'
            chain = 'polygon'
            node_url = 'https://polygon-rpc.com/'
            explorer_url = "https://api.polygonscan.com/api"
            API_KEY = 'D7ZW5H9BW4M8H24E9HV65AUDXM3ZHZTNR8'

        web3 = Web3(Web3.HTTPProvider(node_url))
        abi_endpoint = f"{explorer_url}?module=contract&action=getabi&address={address}&apikey={API_KEY}"
        while True:
            try:
                abi = json.loads(requests.get(abi_endpoint).text)
                break
            except:
                sleep(5)
                continue
     
        txs = fetch_txs(address, chain, node_url, explorer_url, abi)
        
        if len(txs) == 0:
            basetoken_start_liquidity = check_liquidity(address, basetoken_address, node_url, abi, explorer_url)/1000000000000000000
            datatoken_start_liquidity = check_liquidity(address, dataset[1], node_url, abi, explorer_url)/1000000000000000000
            token_ratio = float(basetoken_start_liquidity/datatoken_start_liquidity)
            datatoken_start_price = datatoken_price(address, basetoken_address, datatoken_address, node_url, abi, explorer_url)
            
            dataset.extend([basetoken_start_liquidity, datatoken_start_liquidity, datatoken_start_price, token_ratio])
            datasets_def.extend([[dataset, txn_history]])
            continue
        
                
        for tx in txs:
            tx_data = ast.literal_eval(tx['parameters'])
            tx_type = ''
            basetoken_amount_in = None
            basetoken_amount_out = None
            datatoken_amount_in = None
            datatoken_amount_out = None
            
            if 'joinswap' in  tx['object'].lower():
                tx_type = 'liquidity_in'
                basetoken_amount_in = float(Web3.fromWei(tx_data['tokenAmountIn'], 'ether'))
                
                
            if 'exitswap' in  tx['object'].lower():
                tx_type = 'liquidity_out'
                basetoken_amount_out = float(Web3.fromWei(tx_data['minAmountOut'], 'ether'))
                
            if 'swapexact' in tx['object'].lower():

                if tx_data['tokenInOutMarket'][0].lower() == basetoken_address:
                    tx_type = 'swap_bt_in'
                    basetoken_amount_in = float(Web3.fromWei(int(web3.eth.getTransactionReceipt(tx['hash'])['logs'][2]['data'],16), 'ether'))
                    datatoken_amount_out = float(Web3.fromWei(tx_data['amountsInOutMaxFee'][1], 'ether'))

                if tx_data['tokenInOutMarket'][0].lower() == datatoken_address:
                    tx_type = 'swap_bt_out'
                    datatoken_amount_in = float(Web3.fromWei(tx_data['amountsInOutMaxFee'][0], 'ether'))
                    basetoken_amount_out = float(Web3.fromWei(int(web3.eth.getTransactionReceipt(tx['hash'])['logs'][4]['data'],16), 'ether'))

            txn_history.extend([[tx_type, basetoken_amount_in, basetoken_amount_out, datatoken_amount_in, datatoken_amount_out, tx['sender'], tx['datetime'], tx['block']]])
        basetoken_start_liquidity, datatoken_start_liquidity, datatoken_start_price, token_ratio = starting_liquidity(address, txn_history[0], network) 
        dataset.extend([basetoken_start_liquidity, datatoken_start_liquidity, datatoken_start_price, token_ratio])
        datasets_def.extend([[dataset, txn_history]])

        
    return datasets_def

def create_database(datasets_def):

    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()


    cursor.execute('''CREATE TABLE IF NOT EXISTS datasets_list
                    (id INTEGER PRIMARY KEY, pool_address text, datatoken_address text, network text, liquidity_provider_fee real, swap_fee real, creation_date text, basetoken_start_liquidity real, datatoken_start_liquidity real, datatoken_start_price real, token_ratio real)''')

    conn.commit()

    for dataset_data in datasets_def:
        dataset = dataset_data[0]
        cursor.execute("INSERT INTO datasets_list VALUES(?,?,?,?,?,?,?,?,?,?,?)", (None, dataset[0], dataset[1], dataset[2], dataset[3], dataset[4], dataset[5], dataset[6], dataset[7], dataset[8], dataset[9]))
        conn.commit()

    for dataset_data in datasets_def:
        address = str(dataset_data[0][0])

        query = "CREATE TABLE '"+address+"_txs' (tx_type text, basetoken_amount_in real, basetoken_amount_out real, datatoken_amount_in real, datatoken_amount_out real, sender text, datetime text)"
        cursor.execute(query)
        conn.commit()

        if len(dataset_data[1])==0:
            continue
        
        for tx in dataset_data[1]:
            cursor.execute("INSERT INTO '"+address+"_txs' VALUES(?,?,?,?,?,?,?)", [tx[0], tx[1], tx[2], tx[3], tx[4], tx[5], str(tx[6])])
            conn.commit()
            
    conn.close()
    return True

def store_consumers(address, consumers):
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()

    query = "CREATE TABLE IF NOT EXISTS '"+address+"_consumers' (datetime text)"
    cursor.execute(query)

    for c in consumers:
        query = "INSERT INTO '"+address+"_consumers' (datetime) VALUES (?)"
        cursor.execute(query, (str(c),))
        conn.commit()
    conn.close()

def fetch_consumers():

    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    datatoken_consumers = []

    query = "SELECT pool_address,network FROM datasets_list"
    datasets = cursor.execute(query).fetchall()
    conn.commit()
    conn.close()

    for dataset in datasets:
        
        address = dataset[0]
        network = dataset[1]
        
        consumers = []
        data = {
            "query": "{pool(id: \"%s\", subgraphError: allow) {datatoken {address orders {id}}}}" % address,
            "operationName": None,
            "variables": None,
        }
        
        if network == 'polygon':
            url = 'https://v4.subgraph.polygon.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph'
            r = requests.post(url, data=json.dumps(data)).json()
            
            for tx in r['data']['pool']['datatoken']['orders']:
                datetime = decode_tx_polygon(tx['id'].split('-')[0])
                consumers.append(datetime)
            store_consumers(address, consumers)
                
        if network == 'ethereum':
            url = 'https://v4.subgraph.mainnet.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph'
            r = requests.post(url, data=json.dumps(data)).json()
            
            for tx in r['data']['pool']['datatoken']['orders']:
                datetime = decode_tx_ethereum(tx['id'].split('-')[0])
                consumers.append(datetime)
            store_consumers(address, consumers)
                
    return True


          
def update_ds():
    datasets = update_datasets()
    datasets_def = extract_dataset_data(datasets)
    create_database(datasets_def)
    fetch_consumers()
    
update_ds()
