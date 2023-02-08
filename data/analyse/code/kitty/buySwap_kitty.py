import numpy as np
import csv
import zipfile
import os
import json
import pickle

def getPairAddress(pairAddressMap,pairPath):
    with open(pairPath,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            pairAddress=row[0]
            pairAddressMap[pairAddress]=1

def addFlashbots(flashbotsTxMap):
    file=open("/all_blocks").read()
    all_blocks=json.loads(file)
    for block in all_blocks:
        transactions=block["transactions"]
        for transaction in transactions:
            transaction_hash=transaction["transaction_hash"]
            flashbotsTxMap[transaction_hash]=transaction

# transactionHash,maker,taker,price            
def get_Cost_fromEvent(event_csv,costMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            price=row[3]
            costMap[transactionHash]=price
            

# blockNumber,timestamp,transactionHash,tokenAddress,from,to,fromIsContract,toIsContract,value
def get_mortgage_reward_fromERC20(txMap,flashbotsTxMap,pairAddressMap,costMap,outputPath_dict,outputPath_csv):
    fileDir = "/erc20/";

    files = [
        # "13000000to13249999_ERC20Transaction",
        # "13250000to13499999_ERC20Transaction",
        # "13500000to13749999_ERC20Transaction",
        # "13750000to13999999_ERC20Transaction",
        "14000000to14249999_ERC20Transaction",
        "14250000to14499999_ERC20Transaction",
        "14500000to14749999_ERC20Transaction",
        "14750000to14999999_ERC20Transaction"
    ];

    for file in files:
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        theCSV = theZIP.open(file+".csv");

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            tokenAddress=oneArray[3]
            fromAddress=oneArray[4]
            toAdddress=oneArray[5]
            value=oneArray[8]
            
            try:
                _=flashbotsTxMap[transactionHash]
                
                # mortgage: mortgage NFT into market and get erc20 token
                if fromAddress==zeroAddress and value.rstrip('0')=="1":
                    mortgage="true"
                    if transactionHash not in txMap.keys():
                        txMap_value={}
                    else:
                        txMap_value=txMap[transactionHash]
                    txMap_value["mortgage"]=mortgage
                    txMap[transactionHash]=txMap_value
            except:
                pass
                
            try:
                _=flashbotsTxMap[transactionHash]
                
                # reward: swap erc20 token to weth
                if tokenAddress==weth and pairAddressMap[fromAddress]==1:
                    reward=value
                    if transactionHash not in txMap.keys():
                        txMap_value={}
                    else:
                        txMap_value=txMap[transactionHash]
                    txMap_value["reward"]=reward
                    txMap[transactionHash]=txMap_value
            except:
                pass
            
            try:
                _=flashbotsTxMap[transactionHash]
                
                # cost: price of buying the NFT
                txMap_value=txMap[transactionHash]
                txMap_value["cost"]=costMap[transactionHash]
                txMap[transactionHash]=txMap_value
            except:
                pass
                
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)
        ouputCsv(txMap,flashbotsTxMap,outputPath_csv)
            
# ['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 
# 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed', 
# 'callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas']
def getNormalTransactionFromXblock(txMap,flashbotsTxMap,costMap,outputPath_dict,outputPath_csv):
    print("getNormalTransactionFromXblock")
    fileDir = "/normalTransaction/";

    files = [
		# "13000000to13249999_BlockTransaction",
		# "13250000to13499999_BlockTransaction",
		# "13500000to13749999_BlockTransaction",
		# "13750000to13999999_BlockTransaction",
		"14000000to14249999_BlockTransaction",
		"14250000to14499999_BlockTransaction",
		"14500000to14749999_BlockTransaction",
		"14750000to14999999_BlockTransaction"
    ];

    for file in files:
        print("file",file)
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        theCSV = theZIP.open(file+".csv");

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            gasPrice=oneArray[10]
            gasUsed=oneArray[11]
            isError=oneArray[13]
            try:
                txMap_value=txMap[transactionHash]
                transactionFee=str(int(gasPrice)*int(gasUsed))
                txMap_value["transactionFee"]=transactionFee
                txMap_value["isError"]=isError
            except:
                pass    
                    
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)
        ouputCsv(txMap,flashbotsTxMap,outputPath_csv)
            
            
def ouputCsv(txMap,flashbotsTxMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","transactionFee","coinbase_transfer","total_miner_reward","mortgage","reward","cost"])
    for key,value in txMap.items():
        transactionFee="none"
        coinbase_transfer=flashbotsTxMap[key]["coinbase_transfer"]
        total_miner_reward=flashbotsTxMap[key]["total_miner_reward"]
        
        try:
            transactionFee=value["transactionFee"]
        except:
            pass
        
        if "mortgage" in value.keys() and "reward" in value.keys() and "cost" in value.keys():
            row=[key,transactionFee,coinbase_transfer,total_miner_reward,value["mortgage"],value["reward"],value["cost"]]
            writer.writerow(row)


zeroAddress="0x0000000000000000000000000000000000000000"
weth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
pairAddressMap={}

def main():
    outputPath_dict="/geth/nft_analyse/data/kitty/nftSwap_txMap_14_15.data"
    outputPath_csv="/geth/nft_analyse/data/kitty/nftSwap_14_15.csv"
    pairAddress_csv="/geth/nft_analyse/data/swapDex/pairWithWeth.csv"
    event_csv="/geth/nft_analyse/data/kitty/receipt_14_15.csv"
    flashbotsTxMap={}
    txMap={}
    pairAddressMap={}
    costMap={}
    
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    getPairAddress(pairAddressMap,pairAddress_csv)
    get_Cost_fromEvent(event_csv,costMap)
    get_mortgage_reward_fromERC20(txMap,flashbotsTxMap,pairAddressMap,costMap,outputPath_dict,outputPath_csv)
    getNormalTransactionFromXblock(txMap,flashbotsTxMap,costMap,outputPath_dict,outputPath_csv)
    
if __name__ == '__main__':
    main()