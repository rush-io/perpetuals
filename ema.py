from datetime import datetime, timezone
import orjson
import aiohttp
import asyncio
import pandas as pd
import time
import numpy as np

START_TIME = int(datetime(year=2021, month=1, day=1, hour=0, minute=0).timestamp())
END_TIME = int(datetime(year=2021, month=11, day=18, hour=0, minute=0).timestamp())

async def get(url):
	async with aiohttp.ClientSession(json_serialize=orjson.dumps) as session:
		async with session.get(url) as resp:
			data = await resp.json(loads=orjson.loads)
			return data

async def get_perp_markets(volThreshold):
    PERPS = []
    markets = await get(url = f"https://ftx.com/api/markets")
    _m = markets['result']
    for m in _m:
        if 'PERP' in m['name'] and m['volumeUsd24h'] > volThreshold:
            PERPS.append(m['name'])
    return PERPS
   
async def determine_tuples(start_time, end_time, granularity=15, limit=1500):
    # return array of times to multiprocess / async.gather
    tups = []
    # max return len is 1500 samples
    while end_time - start_time > 0:
        t = (end_time-((limit-1)*granularity),end_time)
        
        tups.append(t)
        end_time -= (limit*granularity)
    return tups

def forwardFillTWAP(dicts, gran, start, limit):
	_arr = np.zeros(limit)
	for i in dicts:
		_p = (i['open']+i['close']+i['low']+i['high'])/4
		_idx = int((i['time'] - start*1e3) // (gran*1e3))
		_arr[_idx] = _p
	return _arr


async def gatherInstance(asset, granularity, tup, limit=1500):

    while True:
        try:
            data = await get(url = f"https://ftx.com/api/markets/{asset}/candles?resolution={granularity}&start_time={tup[0]}&end_time={tup[1]}")
            _d = data['result']
            if len(_d) != 1500:
                _r = forwardFillTWAP(_d, granularity, tup[0], limit)
                return _r
            if not data['result']:
                print('empty list for', asset)
                break
            return data['result']
        except Exception as err:
            print(err)
            continue
        
async def gatherData(asset, granularity, tups):

    arrs = await asyncio.gather(*[gatherInstance(asset,granularity,tup) for tup in tups])
    flattened = [item for subarr in arrs for item in subarr]
    print(asset, 'done.')
    print(len(flattened))
    return arrs

async def main():
    granularity = 15

    current_time = time.time()
    markets = await get_perp_markets(volThreshold=1e7)
    tups = await determine_tuples(START_TIME, END_TIME,granularity=15)

    arrs = await asyncio.gather(*[gatherData(asset, granularity, tups) for asset in ])

    # flattened = [item for subarr in arrs for item in subarr]
    function_time = round(time.time()-current_time,2)

    print('runtime:', function_time,'s')

asyncio.run(main())