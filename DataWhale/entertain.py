import sqlite3
from datetime import datetime, timedelta
import requests
from flask import Flask, request
import statistics

def consumers_price():
    address = '0x25faf893edcef3b1c94029f01a088448669fcb9a'
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

consumers_price()
