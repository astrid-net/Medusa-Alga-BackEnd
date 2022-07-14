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



#THESE ARE GENERAL FUNCTIONS 
def fetch_txs(address, chain, node_url, explorer_url, abi, last_tx):
    txs = []
    txs_data = []
    last_block_found = 0

    MORALIS_API_KEY="myH55EK7qkfufMejJTEijIZUFoav6mLIRhnRaLCvYCQViQHYJn2lavJecapDMSDW"
    headers = {'X-API-Key':f"{MORALIS_API_KEY}"}
    r = requests.get(f"https://deep-index.moralis.io/api/v2/{address}?chain={chain}", headers=headers).json()

    for tx in r['result']:
        if tx['block_timestamp'] == last_tx:
            last_block_found = 1
            break
        if tx['receipt_status'] == '1':
            txs.append(tx)        

    if r['total'] < 100:
        pass
        
    if r['total'] > 100 and last_block_found == 0:
        total_txs = r['total']
        last_block = txs[-1]['block_number']
        n_iter = round(total_txs/100)

        for i in range(n_iter):
            MORALIS_API_KEY="myH55EK7qkfufMejJTEijIZUFoav6mLIRhnRaLCvYCQViQHYJn2lavJecapDMSDW"
            headers = {'X-API-Key':f"{MORALIS_API_KEY}"}
            r = requests.get(f"https://deep-index.moralis.io/api/v2/{address}?chain={chain}&to_block={last_block}", headers=headers).json()

            for tx in r['result']:
                if tx['block_timestamp'] == last_tx:
                    break
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
    

######FUNCTION FOR EXISTING DATASETS
def update_txs():
    datasets = []
    
    this_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(this_dir, 'Data/ocean_data.db')
    conn = sqlite3.connect(file_path)
    
    cursor = conn.cursor()
    query = "SELECT * FROM datasets_list"
    
    datasets = [dataset for dataset in cursor.execute(query).fetchall()]
    conn.commit()

    for dataset in datasets:
        #UPDATE TRANSACTIONS

        txn_history = []        
        datatoken_address = dataset[2]
        address = dataset[1]
        fee = float(dataset[4])+float(dataset[5])
        query = "SELECT * FROM '"+address+"_txs'"
        try:
            last_tx = cursor.execute(query).fetchall()[-1][6]
        except:
            continue
        
        conn.commit()
        
        
        if dataset[3] == 'ethereum':
            basetoken_address = '0x967da4048cD07aB37855c090aAF366e4ce1b9F48'
            chain = 'eth'
            node_url = "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161"
            explorer_url ="https://api.etherscan.io/api"
            API_KEY="XS54T9YEXVT2RPXBWDJHUT5D6DCSIGWF73"
            
        if dataset[3] == 'polygon':
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
            except Exception as e:
                sleep(5)
                continue
            
        txs = fetch_txs(address, chain, node_url, explorer_url, abi, last_tx)
        try:
            print(address, len(txs))
        except:
            continue
        
        if len(txs) == 0:
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

        for tx in txn_history:
            query = "INSERT INTO '"+address+"_txs' VALUES (?,?,?,?,?,?,?)"
            cursor.execute(query, [tx[0], tx[1], tx[2], tx[3], tx[4], tx[5], tx[6]])

            
    conn.close()

def store_consumers(address, consumers_to_insert):
    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()

    query = "CREATE TABLE IF NOT EXISTS '"+address+"_consumers' (datetime text)"
    cursor.execute(query)

    for c in reversed(consumers_to_insert):
        query = "INSERT INTO '"+address+"_consumers' (datetime) VALUES (?)"
        cursor.execute(query, (str(c),))
        conn.commit()
    conn.close()

def update_consumers():

    conn = sqlite3.connect('Data/ocean_data.db')
    cursor = conn.cursor()
    datatoken_consumers = []

    query = "SELECT pool_address,network FROM datasets_list"
    datasets = cursor.execute(query).fetchall()
    conn.commit()

    for dataset in datasets:
        print(dataset)
        
        address = dataset[0]
        network = dataset[1]

        try:      
            query = "SELECT * FROM '%s_consumers'"%address
            last_consume = cursor.execute(query).fetchall()[-1][0]
            last_consume = datetime.fromisoformat(last_consume)
            
        except Exception as e:
            print(e)
            continue
        
        consumers = []
        consumers_to_insert = []
        
        data = {
            "query": "{pool(id: \"%s\", subgraphError: allow) {datatoken {address orders {id}}}}" % address,
            "operationName": None,
            "variables": None,
        }
        
        if network == 'polygon':
            url = 'https://v4.subgraph.polygon.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph'
            r = requests.post(url, data=json.dumps(data)).json()
            
            for tx in r['data']['pool']['datatoken']['orders']:
                date = decode_tx_polygon(tx['id'].split('-')[0])
                consumers.append(date)
                
        if network == 'ethereum':
            url = 'https://v4.subgraph.mainnet.oceanprotocol.com/subgraphs/name/oceanprotocol/ocean-subgraph'
            r = requests.post(url, data=json.dumps(data)).json()
            
            for tx in r['data']['pool']['datatoken']['orders']:
                date = decode_tx_ethereum(tx['id'].split('-')[0])
                consumers.append(date)
        for c in reversed(consumers):
            if c == last_consume:
                break
            consumers_to_insert.append(c)

        if len(consumers_to_insert) == 0:
            continue
        else:
            print(consumers_to_insert)
            store_consumers(address, consumers_to_insert)
            
        sleep(3)
    
    conn.close()
                
    return True

def update_data():
    update_txs()
    update_consumers()
update_data()
