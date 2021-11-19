from blocknative.stream import Stream
import json

global_filters = global_filters = [
    {"contractCall.methodName": "addLiquidity" },
    {"contractCall.methodName": "addLiquidityETH" }
]

# Initialize the stream
stream = Stream('47845801-21ab-46e6-b7d9-ecce8ae3be5b', global_filters=global_filters)

# Define your transaction handler which has the context of a specific subscription.
async def txn_handler(txn, unsubscribe):
    if txn['status'] == "confirmed":
        # Output the transaction data to the console
        print(json.dumps(txn, indent=4))

        # Unsubscribe from this subscription
        unsubscribe()

# Define the address you want to watch (uniswap and sushiswap routers)
uniswap_v2_address = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
sushiwap_address = '0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F'

try:
    # Register the subscription
    stream.subscribe_address(uniswap_v2_address, txn_handler)
    stream.subscribe_address(sushiwap_address, txn_handler)

    # Start the websocket connection and start receiving events!
    stream.connect()

except KeyboardInterrupt:
    pass