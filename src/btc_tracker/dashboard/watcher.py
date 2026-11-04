from typing import Any
import asyncio, json
import aiohttp
import websockets
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


MEMPOOL_WSS = "wss://mempool.space/api/v1/ws"
# # websocket btc mempool
# async def listen_mempool():
#     while True:
#         async with websockets.connect(MEMPOOL_WSS) as ws:
#                 # await ws.send(json.dumps({"action": "init"}))
#                 await ws.send(json.dumps({"action": "want", "data": ["blocks",
#                 "mempool-blocks"]}))
#                 async for message in ws:
#                     mesg_json = json.loads(message)
#                     # print the keys of the json message
#                     print(f"keys: {mesg_json.keys()}")
                    
#                     # printing block and the total mempool-size
#                     if  mesg_json.get("blocks"):
#                         for x in mesg_json['blocks']:
#                             toronto = datetime.fromtimestamp(x['timestamp'], ZoneInfo("America/Toronto"))
#                             print(f"height {x['height']} | tx {x['tx_count']} | {toronto}")
                    

#                     # new blocks in message
#                     if mesg_json.get("block"):
#                             print(f"new block: {mesg_json['block']['height']} | {mesg_json['block']['tx_count']} txs | {datetime.fromtimestamp(mesg_json['block']['timestamp'], ZoneInfo('America/Toronto'))}")
#                     print(f"-x-x-x-x-x-x"*3)
#                     if  mesg_json.get("mempool-blocks"):
#                             pass
#                         # print(f"mempool-size -  {mesg_json['mempool-blocks']}" )
#                         # print(mesg_json.get("mempoolInfo", {}).get("size"), mesg_json["mempool-blocks"][0]["nTx"])
                    

async def listen_mempool(on_block):
    while True:
        try:
            async with websockets.connect(MEMPOOL_WSS) as ws:
                await ws.send(json.dumps({"action": "want", "data": ["blocks", "mempool-blocks"]}))
                async for msg in ws:
                    m = json.loads(msg)
                    if m.get("block"): 
                        await on_block(m["block"]['height'],m)
                    if m.get("mempool-blocks"):
                        await on_block(None, m)

        except Exception as e:
            print(f"Error in listen_mempool: {e}")
            await asyncio.sleep(5)  # wait before reconnecting  