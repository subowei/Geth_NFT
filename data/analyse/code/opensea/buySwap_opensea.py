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

# transactionHash,maker,taker,price            
def get_Cost_fromEvent(OrdersMatched_csv,txMap):
    with open(OrdersMatched_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            price=row[3]
            try:
                txMap_value=txMap[transactionHash]
            except:
                txMap_value={}
            txMap_value["cost"]=price
            txMap[transactionHash]=txMap_value
            
            
def get_flashloanFee_fromEvent(OrdersMatched_csv,txMap):
    with open(OrdersMatched_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            premium=row[6]
            
            try:
                txMap_value=txMap[transactionHash]
            except:
                txMap_value={}
            txMap_value["premium"]=premium
            txMap[transactionHash]=txMap_value
            

# blockNumber,timestamp,transactionHash,tokenAddress,from,to,fromIsContract,toIsContract,value
def get_mortgage_reward_fromERC20(txMap,flashbotsTxMap,pairAddressMap,outputPath_dict,outputPath_csv):
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
                
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)
        ouputCsv(txMap,flashbotsTxMap,outputPath_csv)
            
# 第三步骤：获取交易对应的外部to地址，交易手续费
# ['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 
# 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed', 
# 'callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas']
def getNormalTransactionFromXblock(txMap,flashbotsTxMap,outputPath_dict,outputPath_csv):
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
        curBlockNumber=0
        position=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            gasPrice=oneArray[10]
            gasUsed=oneArray[11]
            isError=oneArray[13]
            blockNumber=oneArray[0]
            timestamp=oneArray[1]
            
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0
            
            try:
                txMap_value=txMap[transactionHash]
                transactionFee=str(int(gasPrice)*int(gasUsed))
                txMap_value["transactionFee"]=transactionFee
                txMap_value["isError"]=isError
                txMap_value["blockNumber"]=blockNumber
                txMap_value["position"]=position
                txMap_value["timestamp"]=timestamp
                
            except:
                pass    
                    
            position+=1
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)
        ouputCsv(txMap,flashbotsTxMap,outputPath_csv)
            
            
def ouputCsv(txMap,flashbotsTxMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position","transactionFee","coinbase_transfer","mortgage","reward","cost","premium"])
    for key,value in txMap.items():
        transactionFee="none"
        blockNumber="none"
        position="none"
        timestamp="none"
        premium="none"
        
        try:
            coinbase_transfer=flashbotsTxMap[key]["coinbase_transfer"]
        except:
            continue
        
        try:
            transactionFee=value["transactionFee"]
            blockNumber=value["blockNumber"]
            position=value["position"]
            timestamp=value["timestamp"]
        except:
            pass
        
        try:
            premium=value["premium"]
        except:
            pass
        
        if "mortgage" in value.keys() and "reward" in value.keys() and "cost" in value.keys():
            row=[key,blockNumber,timestamp,position,transactionFee,coinbase_transfer,value["mortgage"],value["reward"],value["cost"],premium]
            writer.writerow(row)


zeroAddress="0x0000000000000000000000000000000000000000"
weth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
openSea_wyvern_exchange="0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b".lower()
pairAddressMap={}

def main():
    outputPath_dict="/geth/nft_analyse/data/opensea/nftSwap_txMap_14_15.data"
    outputPath_csv="/geth/nft_analyse/data/opensea/nftSwap_14_15.csv"
    pairAddress_csv="/geth/nft_analyse/data/swapDex/pairWithWeth.csv"
    OrdersMatched_csv="/geth/nft_analyse/data/opensea/receipt_14_15.csv"
    flashloan_csv="/geth/nft_analyse/data/aave/receipt_flashLoan.csv"
    flashbotsTxMap={}
    txMap={}
    pairAddressMap={}
    
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    getPairAddress(pairAddressMap,pairAddress_csv)
    get_Cost_fromEvent(OrdersMatched_csv,txMap)
    get_flashloanFee_fromEvent(flashloan_csv,txMap)
    get_mortgage_reward_fromERC20(txMap,flashbotsTxMap,pairAddressMap,outputPath_dict,outputPath_csv)
    getNormalTransactionFromXblock(txMap,flashbotsTxMap,outputPath_dict,outputPath_csv)
    
if __name__ == '__main__':
    main()