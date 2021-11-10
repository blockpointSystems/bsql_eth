# bsql_eth
A python application for streaming Ethereum data to a bSQL database. 

## main.py
The script is composed of 4 methods:

`main` Creates a new web3 filter for the latest block and begins the loop, specifying at what interval the loop will run (every 2 seconds)

`log_loop` Checks for new entries every interval. When a new entry exists it is handled.

`handle_event` Prints the block number to the console and sends an insert statement to the database specified in the `conn`.

## example_queries.bsql
Various example queries for interacting with **block** after being populated with data.
