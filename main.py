# This is a python script for streaming Ethereum blocks to the bSQL database
from web3 import Web3
import mdb_bp
import asyncio
import datetime

# Globally define our connections to Infura and bSQL
infru = "https://mainnet.infura.io/v3/your_project_id"
web3 = Web3(Web3.HTTPProvider(infru))
conn = mdb_bp.driver.connect(
    username="your username",
    password="your password",
    connection_protocol="tcp",
    server_address="server address",
    server_port=5461,
    database_name="eth",
    parameters={"interpolateParams": True},
)


# asynchronous defined function to loop
# this loop sets up an event filter and is looking for new entires for the "PairCreated" event
# this loop runs on a poll interval
async def log_loop(event_filter, poll_interval):
    while True:
        for PairCreated in event_filter.get_new_entries():
            handle_event(web3.eth.get_block(PairCreated))
        await asyncio.sleep(poll_interval)


# define function to handle events and print to the console
def handle_event(block):
    print(block['number'])
    conn.exec("INSERT blocks VALUES (" +
              "\"" + str(datetime.datetime.utcfromtimestamp(block['timestamp'])) + "\"," +
              str(block['number']) + "," +
              "\"" + str(block['hash'].hex()) + "\"," +
              "\"" + str(block['parentHash'].hex()) + "\"," +
              "\"" + str(block['nonce'].hex()) + "\"," +
              "\"" + str(block['sha3Uncles'].hex()) + "\"," +
              "\"" + str(block['logsBloom'].hex()) + "\"," +
              "\"" + str(block['transactionsRoot'].hex()) + "\"," +
              "\"" + str(block['stateRoot'].hex()) + "\"," +
              "\"" + str(block['receiptsRoot'].hex()) + "\"," +
              "\"" + str(block['miner']) + "\"," +
              str(block['difficulty']) + "," +
              str(block['size']) + "," +
              "\"" + str(block['extraData'].hex()) + "\"," +
              str(block['gasLimit']) + "," +
              str(len(block['transactions'])) + "," +
              str(block['baseFeePerGas']) + ")")


# when main is called
# create a filter for the latest block and look for the "PairCreated" event for the uniswap factory contract
# run an async loop
# try to run the log_loop function above every 2 seconds
def main():
    block_filter = web3.eth.filter('latest')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(block_filter, 2)))
    finally:
        # close loop to free up system resources
        loop.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
