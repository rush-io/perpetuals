import requests
import json

PERPETUALS = {}

def generate_perpetual_symbols(dataset):

    # binance
    # start with binance because i think they have most perpetual markets, just throw them all in there
    binance_usdt = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo').json()['symbols']
    binance_inverse = requests.get('https://dapi.binance.com/dapi/v1/exchangeInfo').json()['symbols']
    for contract in binance_usdt:
        if contract['quoteAsset'] == 'USDT' and contract['contractType'] == 'PERPETUAL':
            dataset[contract['baseAsset']] = {'BINANCE_USDT':contract['symbol']}
    for contract in binance_inverse:
        if contract['contractType'] == 'PERPETUAL':
            if contract['baseAsset'] not in dataset:
                dataset[contract['baseAsset']] = {'BINANCE_INVERSE':contract['symbol']}
            else:
                dataset[contract['baseAsset']]['BINANCE_INVERSE'] = contract['symbol']

    # ftx
    ftx = requests.get('https://ftx.com/api/markets').json()['result']
    for contract in ftx:
        if 'PERP' in contract['name']:
            if contract['underlying'] not in dataset:
                dataset[contract['underlying']] = {'FTX':contract['name']}
            else:
                dataset[contract['underlying']]['FTX'] = contract['name']

    # bitmex
    bitmex = requests.get('https://www.bitmex.com/api/v1/instrument/activeIntervals').json()
    for interval,symbol in zip(bitmex['intervals'], bitmex['symbols']):
        base_asset = interval.split(':')[0]
        if 'perpetual' in interval:
            if base_asset not in dataset:
                if base_asset == 'XBT':
                    dataset['BTC']['BITMEX'] = 'XBTUSD'
            else:
                dataset[base_asset]['BITMEX'] = symbol

    # okex
    okex = requests.get('https://www.okex.com/api/swap/v3/instruments/').json()
    for contract in okex:
        if contract['underlying_index'] not in dataset:
            if contract['quote_currency'] == 'USDT':
                dataset[contract['underlying_index']] = {'OKEX_USDT':contract['instrument_id']}
            else:
                dataset[contract['underlying_index']] = {'OKEX_INVERSE':contract['instrument_id']}
        else:
            if contract['quote_currency'] == 'USDT':
                dataset[contract['underlying_index']]['OKEX_USDT'] = contract['instrument_id']
            else:
                dataset[contract['underlying_index']]['OKEX_INVERSE'] = contract['instrument_id']

    # huobi
    huobi_usdt = requests.get('https://api.hbdm.com/linear-swap-api/v1/swap_contract_info').json()['data']
    huobi_inverse = requests.get('https://api.hbdm.com/swap-api/v1/swap_contract_info').json()['data']
    for contract in huobi_usdt:
        if contract['symbol'] not in dataset:
            dataset[contract['symbol']] = {'HUOBI_USDT':contract['contract_code']}
        else:
            dataset[contract['symbol']]['HUOBI_USDT'] = contract['contract_code']
    for contract in huobi_inverse:
        if contract['symbol'] not in dataset:
            dataset[contract['symbol']] = {'HUOBI_INVERSE':contract['contract_code']}
        else:
            dataset[contract['symbol']]['HUOBI_INVERSE'] = contract['contract_code']   

    # bybit
    bybit = requests.get('https://api.bybit.com/v2/public/tickers').json()['result']
    for contract in bybit:
        if contract['delivery_time'] == '':
            symbol = contract['symbol'].split('USD')
            if symbol[0] not in dataset:
                if symbol[1] == '':
                    dataset[symbol[0]] = {'BYBIT_INVERSE':contract['symbol']}
                else:
                    dataset[symbol[0]] = {'BYBIT_USDT':contract['symbol']}
            else:
                if symbol[1] == '':
                    dataset[symbol[0]]['BYBIT_INVERSE'] = contract['symbol']
                else:
                    dataset[symbol[0]]['BYBIT_USDT'] = contract['symbol']

    # deribit (there's only eth and btc, so i can't be fucked bruv. im just putting them hoes in there until some shit happens)
    dataset['BTC']['DERIBIT'] = 'BTC-PERPETUAL'
    dataset['ETH']['DERIBIT'] = 'ETH-PERPETUAL'

    return dataset

def write_to_file_or_gay(dataset):
    with open('perpetuals.txt', 'w') as convert_file:
     convert_file.write(json.dumps(dataset))

contracts = generate_perpetual_symbols(dataset=PERPETUALS)
write_to_file_or_gay(dataset=contracts)