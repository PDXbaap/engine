# README.md
###Use command

```
make pdxc
```
###Create json file

first create json file(genesis.json)

```
{
  "config": {
    "chainId": 738,
    "homesteadBlock": 0,
    "eip155Block": 0,
    "eip158Block": 0,
    "utopia": {
      "epoch": 30000,
      "noRewards":false,
      "cfd":5,
      "numMasters":4,
      "blockDelay":2000,
      "tokenChain": [
        {
          "chainId": "739",
          "chainOwner": "123456",
          "tokenSymbol": "15432523",
          "enodes": [
            "enode://3e3e179197a47a53dded4522de140cfcb8e956f262bc443d8e314d7401ed64c4a8e8c08839eb58a640a4f3e7593d791b34b8e0992a2ed66f1995fcbc98aae8a6@http://192.168.1.229:12181",
            "enode://3e3e179197a47a53dded4522de140cfcb8e956f262bc443d8e314d7401ed64c4a8e8c08839eb58a640a4f3e7593d791b34b8e0992a2ed66f1995fcbc98aae8a6@http://192.168.1.228:12181"
          ],
          "rpcHosts": [
            "http://127.0.0.1:8545",
            "http://127.0.0.1:8545"
          ]
        }
      ]
    }
  },
  "alloc": {
    "B735C47be630634C86FeE87901e03287F57Ebc07": {
      "balance": "3000000000000000000"
    }
  },
  "nonce": "0x0000000000000042",
  "difficulty": "0x1000",
  "mixhash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "coinbase": "0x0000000000000000000000000000000000000000",
  "timestamp": "0x00",
  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "extraData": "",
  "gasLimit": "4712388"
}

```
Description:

```
config{

chainId

  //Version statement
  "homesteadBlock": 0,
  "eip155Block"   : 0,
  "eip158Block"   : 0,
}

utopia{
   noRewards //whether reward for generating a block
   cfd:5, //the distance in blocks (configurable).
   numMasters:4, //master number
   blockDelay:2000, //blocking time
   tokenChain: //basic service chain information
   alloc: //initialize the number of account tokens
}
   //genesis block information
   "nonce": "0x0000000000000042",
   "difficulty": "0x1000",
   "mixhash": "0x0000000000000000000000000000000000000000000000000000000000000000",
   "coinbase": "0x0000000000000000000000000000000000000000",
   "timestamp": "0x00",
   "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
   "extraData": "",
   "gasLimit": "4712388"

```

###Make datadir directory
Use the above json file to initialize the node and store the file in the corresponding path.

The initialization command is as follows:

```
init
/Users/liu/XXXXXXX/genesis.json
--datadir
/Users/liu/XXXXXXX/data1
```
###Create an account address
After the initialization is successful, the Keystore folder will appear in the corresponding path to store the account private key. 

Use the geth command to create an account.

```
pdxc account new --datadir "/Users/liu/XXXXXXX/data1"

```

After the creation is complete, you need to unlock the account and create a password.txt file to store the account password.

When everything is ready, start running PDX,

Note that the account address should start with 0x. The specific command can be viewed with the -h command.

The specific running command is as follows:

```
./pdxc --pprof --datadir /Users/liu/XXXXXXX/data2 --mine --minerthreads=2 --networkid 1218 --rpcaddr 0.0.0.0 --rpc --rpccorsdomain "*" --port 12182 --verbosity 5 --ethash.dagdir /Users/liu/XXXXXXX/dagdir --etherbase 0x7de2a31d6ca36302ea7b7917c4fc5ef4c12913b6 --ipcdisable --rpcport 8546  --nodiscover --nat none --unlock 0x7de2a31d6ca36302ea7b7917c4fc5ef4c12913b6 --password /Users/liu/XXXXXXX/password.txt --ccRpcPort 12344

```
Need to configure the environment variable: export PDX_BAAP_HOME="your path /pdx-chain/pdxcc"


