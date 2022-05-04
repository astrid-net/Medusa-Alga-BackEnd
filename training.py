import requests as req
from flask import Flask, render_template
from flask import jsonify
from flask import request
import sys
import logging
from market_data import *

app = Flask(__name__, static_url_path='/static')
app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.ERROR)

@app.route('/token_holders_count/', methods=['GET'])
def TokenHolders():
    datasets = {
        'dataset1': {
                'title': 'Bitcoin historical data 2012-2020 1min intervals',
                'code': 'did:op:4B4B3605eE850f041Ba4A817A79B2DF04A392CD4',
                'pool': '0xfdd3d696c6328a7fd1d68e00cd8f59aa315dcd93',
                'datatoken': '0x4B4B3605eE850f041Ba4A817A79B2DF04A392CD4',
            },

        'dataset2': {
            'title': 'Active Multi-Year Instagram User Data | 1.7mil Bytes',
            'code': 'did:op:1BCCB217E9b902c85976CC72050B13d3CBa30b43',
            'pool': '0xc7ddf111d47cffd50dd4909c50d7412108e584be',
            'datatoken': '0x1BCCB217E9b902c85976CC72050B13d3CBa30b43',
            },

        'dataset3': {
            'title': 'The Ocean',
            'code': 'did:op:5F25016925C9883b4379F7B7D3D69EC00aC10964',
            'pool': '0x1686D247A12A246CAf8DFCB6e099636F2c3C2763',
            'datatoken': '0x5F25016925C9883b4379F7B7D3D69EC00aC10964',
            },

        'dataset4': {
            'title': 'CO2 Accounting in Copenhagen',
            'code': 'did:op:0E56c49d3013AcA0D6ee2aFDF5642c80F642D741',
            'pool': '0x441780bb38d8c372f85d3ef15db60e2faa3e72cd',
            'datatoken': '0x0E56c49d3013AcA0D6ee2aFDF5642c80F642D741',
            },

        'dataset5': {
            'title': 'Rug Pull',
            'code': 'did:op:8b4aE8d4C4925EfF68fEF5E12C923Dd3243a0E15',
            'pool': '0x0a8a7eb302da32132bbaa9d1925a4e6bea52818c',
            'datatoken': '0x8b4aE8d4C4925EfF68fEF5E12C923Dd3243a0E15',
            },

        }


    for dataset in datasets.values():
        if dataset['code'] == code:
            r = req.get('https://api.ethplorer.io/getTokenInfo/'+dataset['datatoken']+'?apiKey=freekey').json()

            response = {
                'name': r['name'],
                'holders_count': r['holdersCount'],
                'dataset': dataset
            }
            return response
        else:
            response = {'No datatoken found in the database.'}
            return response
