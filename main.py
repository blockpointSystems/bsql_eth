# This is a sample Python script.
from web3 import Web3
import mdb_bp
import asyncio
import datetime

infru = "https://mainnet.infura.io/v3/50f9f82ce07e451c9abe4cd40d72cd60"
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
    # import time
    # import datetime
    #
    # infru = "https://mainnet.infura.io/v3/50f9f82ce07e451c9abe4cd40d72cd60"
    # web3 = Web3(Web3.HTTPProvider(infru))
    # print(web3.isConnected())
    # block = web3.eth.get_block('latest')
    # print(str(block['number']))
    # conn = mdb_bp.driver.connect(
    #     username="system",
    #     password="biglove",
    #     connection_protocol="tcp",
    #     server_address="localhost",
    #     server_port=5461,
    #     database_name="eth",
    #     parameters={"interpolateParams": True},
    # )
    # rows = conn.query("SELECT * FROM blocks")
    # itr = iter(rows)
    # for row in itr:
    #     print(row)
    #
    # # date = datetime.datetime.utcfromtimestamp(block['timestamp'])
    # # print(str(date))
    # # print(str(block['number']))
    # # print(str(block['hash'].hex()))
    # # print(str(block['parentHash'].hex()))
    # # print(str(block['nonce'].hex()))
    # # print(str(block['sha3Uncles'].hex()))
    # # print(str(block['logsBloom'].hex()))
    # # print(str(block['transactionsRoot'].hex()))
    # # print(str(block['stateRoot'].hex()))
    # # print(str(block['receiptsRoot'].hex()))
    # # print(str(block['miner']))
    # # print(str(block['difficulty']))
    # # print(str(block['size']))
    # # print(str(block['extraData'].hex()))
    # # print(str(block['gasLimit']))
    # # print(str(block['baseFeePerGas']))
    # # print(str(len(block['transactions'])))
    # # print(str(block['baseFeePerGas']))
    #
    # # print("INSERT blocks VALUES (" +
    # #       "\"" + str(datetime.datetime.utcfromtimestamp(block['timestamp'])) + "\"," +
    # #       str(block['number']) + "," +
    # #       "\"" + str(block['hash'].hex()) + "\"," +
    # #       "\"" + str(block['parentHash'].hex()) + "\"," +
    # #       "\"" + str(block['nonce'].hex()) + "\"," +
    # #       "\"" + str(block['sha3Uncles'].hex()) + "\"," +
    # #       "\"" + str(block['logsBloom'].hex()) + "\"," +
    # #       "\"" + str(block['transactionsRoot'].hex()) + "\"," +
    # #       "\"" + str(block['stateRoot'].hex()) + "\"," +
    # #       "\"" + str(block['receiptsRoot'].hex()) + "\"," +
    # #       "\"" + str(block['miner']) + "\"," +
    # #       str(block['difficulty']) + "," +
    # #       str(block['size']) + "," +
    # #       "\"" + str(block['extraData'].hex()) + "\"," +
    # #       str(block['gasLimit']) + "," +
    # #       str(block['baseFeePerGas']) + "," +
    # #       str(len(block['transactions'])) + "," +
    # #       str(block['baseFeePerGas']) + ")")
    #
    # conn.exec("INSERT blocks VALUES (" +
    #       "\"" + str(datetime.datetime.utcfromtimestamp(block['timestamp'])) + "\"," +
    #       str(block['number']) + "," +
    #       "\"" + str(block['hash'].hex()) + "\"," +
    #       "\"" + str(block['parentHash'].hex()) + "\"," +
    #       "\"" + str(block['nonce'].hex()) + "\"," +
    #       "\"" + str(block['sha3Uncles'].hex()) + "\"," +
    #       "\"" + str(block['logsBloom'].hex()) + "\"," +
    #       "\"" + str(block['transactionsRoot'].hex()) + "\"," +
    #       "\"" + str(block['stateRoot'].hex()) + "\"," +
    #       "\"" + str(block['receiptsRoot'].hex()) + "\"," +
    #       "\"" + str(block['miner']) + "\"," +
    #       str(block['difficulty']) + "," +
    #       str(block['size']) + "," +
    #       "\"" + str(block['extraData'].hex()) + "\"," +
    #       str(block['gasLimit']) + "," +
    #       str(len(block['transactions'])) + "," +
    #       str(block['baseFeePerGas']) + ")")
    # conn.close()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
