## Building the source

Building `Geth` requires both a Go (version 1.16 or later) and a C compiler. You can install
them using your favourite package manager. Once the dependencies are installed, run

```shell
make geth
```

```shell
./build/bin/geth import "importPath"  --datadir "destPath"
```

## Document Description

### Geth main part

1. core/blockchain.go

    The core of Geth. Includes NFT Flow Extractor.

2. core/state_processor.go

    Sorted Modult. Sort transactions by nonce and tip.


### Geth output processing part

#### code
1. code/nftFlow

    Geth output. Detect NFT-MEV.

2. code/opensea/buySwap_opensea.py, code/punk/buySwap_punk.py, code/kitty/buySwap_kitty.py

    Detect BuySwap Arbitrage

3. code/looksRare/buySell_looksRare.py

    Detect BuySell Arbitrage

4. code/kitty/giveBirth.py

    Detect GiveBirth Arbitrage

5. code/punk/sandwich_punk.py

    Detect Sandwich Buying

#### data

1. data/nftFlow/nftFlow.csv

    Geth output

    (1) columns: 

        blockNum,tokenAddress,tokenId,startAddr,tokenIdOwnerAddrOriginal,tokenIdOwnerAddrEdited,endAddr_0,transactionHash_0,positionOriginal_0,positionEdited_0,endAddr_1,transactionHash_1,positionOriginal_1,positionEdited_1,timestamp,isError_0,transactionFee_0,from_0,to_0,isError_1,transactionFee_1,from_1,to_1,coinbase_transfer_0,coinbase_transfer_1

    (2) explanation:

        blockNum
        NFT smart contract address
        NFT ID
        initial address in events
        NFT owner address in original order
        NFT owner address in edited order
        end address in events of transaction_0
        hash of transaction_0, which is also named attacker transaction
        position of transaction_0 in original order
        position of transaction_0 in edited order
        end address in events of transaction_1
        hash of transaction_1, which is also named attacker transaction
        position of transaction_1 in original order
        position of transaction_1 in edited order

2.  data/opensea/buySwap_opensea.csv, 
    data/punk/buySwap_punk.csv
    data/kitty/buySwap_kitty.csv

    BuySwap Arbitrage

    (1) columns: 

        transactionHash,blockNumber,timestamp,position,transactionFee,coinbase_transfer,mortgage,reward,cost,premium

    (2) explanation:

        transaction hash
        block number (height)
        timestamp of transactionHash
        position of position
        transaction fee
        fee directly transferred to the miner through coinbase.transfer()
        mortgage action
        reward from exchange market
        cost of buying NFT
        premium of using flash


3. data/looksRare/buySell.csv

    BuySell Arbitrage

    (1) columns: 

        transactionHash,blockNumber,timestamp,position,transactionFee,coinbase_transfer,reward


    (2) explanation:

        transaction hash
        block number (height)
        timestamp of transactionHash
        position of position
        transaction fee
        fee directly transferred to the miner through coinbase.transfer()
        price difference between buying and selling NFT

4. data/kitty/giveBirth_kitty.csv

    GiveBirth Arbitrage

5. data/punk/sandwichBuy_punk.csv

    Sandwich Buying