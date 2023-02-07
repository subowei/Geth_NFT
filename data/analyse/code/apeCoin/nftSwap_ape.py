import numpy as np
import csv    #加载csv包便于读取csv文件
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
            
def get_mint_fromEvent(event_csv,flashbotsTxMap,txMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            
            if len(flashbotsTxMap)==0:
                if transactionHash not in txMap.keys():
                    txMap_value={}
                else:
                    txMap_value=txMap[transactionHash]
                txMap_value["mint"]="true"
                txMap[transactionHash]=txMap_value
            else:
                try:
                    _=flashbotsTxMap[transactionHash]
                    
                    if transactionHash not in txMap.keys():
                        txMap_value={}
                    else:
                        txMap_value=txMap[transactionHash]
                    txMap_value["mint"]="true"
                    txMap[transactionHash]=txMap_value
                except:
                    pass
            
def get_redeem_fromEvent(event_csv,flashbotsTxMap,txMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            
            if len(flashbotsTxMap)==0:
                if transactionHash not in txMap.keys():
                    txMap_value={}
                else:
                    txMap_value=txMap[transactionHash]
                txMap_value["redeem"]="true"
                txMap[transactionHash]=txMap_value
            else:
                try:
                    _=flashbotsTxMap[transactionHash]
                    
                    if transactionHash not in txMap.keys():
                        txMap_value={}
                    else:
                        txMap_value=txMap[transactionHash]
                    txMap_value["redeem"]="true"
                    txMap[transactionHash]=txMap_value
                except:
                    pass
            
            
def get_claim_fromEvent(event_csv,flashbotsTxMap,txMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            
            if len(flashbotsTxMap)==0:
                if transactionHash not in txMap.keys():
                    txMap_value={}
                else:
                    txMap_value=txMap[transactionHash]
                txMap_value["claim"]="true"
                txMap[transactionHash]=txMap_value
            else:
                try:
                    _=flashbotsTxMap[transactionHash]
                    
                    if transactionHash not in txMap.keys():
                        txMap_value={}
                    else:
                        txMap_value=txMap[transactionHash]
                    txMap_value["claim"]="true"
                    txMap[transactionHash]=txMap_value
                except:
                    pass
            

# blockNumber,timestamp,transactionHash,tokenAddress,from,to,fromIsContract,toIsContract,value
def get_reward_fromERC20(txMap,flashbotsTxMap,pairAddressMap,outputPath_dict,outputPath_csv):
    fileDir = "/";

    files = [
        "13000000to13249999_ERC20Transaction",
        "13250000to13499999_ERC20Transaction",
        "13500000to13749999_ERC20Transaction",
        "13750000to13999999_ERC20Transaction"
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
def getNormalTransactionFromXblock(txMap,flashbotsTxMap,costMap,outputPath_dict,outputPath_csv):
    print("getNormalTransactionFromXblock")
    fileDir = "/";

    files = [
		"13000000to13249999_BlockTransaction",
		"13250000to13499999_BlockTransaction",
		"13500000to13749999_BlockTransaction",
		"13750000to13999999_BlockTransaction",
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
            
            
# def ouputCsv(txMap,flashbotsTxMap,outputPath_csv):
#     f = open(outputPath_csv,'w')
#     writer = csv.writer(f)
#     writer.writerow(["transactionHash","transactionFee","coinbase_transfer","claim","redeem","mint","reward"])
#     for key,value in txMap.items():
#         transactionFee="none"
#         coinbase_transfer="none"
#         coinbase_transfer="none"
#         try:
#             coinbase_transfer=flashbotsTxMap[key]["coinbase_transfer"]
#             transactionFee=value["transactionFee"]
#         except:
#             pass
        
#         if "redeem" in value.keys() and "claim" in value.keys() and "mint" in value.keys() and "reward" in value.keys():
#             row=[key,transactionFee,coinbase_transfer,value["claim"],value["redeem"],value["mint"],value["reward"]]
#             writer.writerow(row)
            
def ouputCsv(txMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash"])
    for key,value in txMap.items():
        if "redeem" in value.keys() and "claim" in value.keys() and "mint" in value.keys():
            row=[key]
            writer.writerow(row)

# 定义范式
'''
收益-reward
weth:
    pair_kitty_weth-> A
    
确认存在抵押行为-mortgage
WrappedG0:
    zeroAddress -> A
    
成本-cost
Ether:
    A -> 交易所地址
'''

zeroAddress="0x0000000000000000000000000000000000000000"
weth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
pairAddressMap={}

def main():
    outputPath_dict="/geth/nft_analyse/data/apeCoin/nftSwap_txMap.data"
    outputPath_csv="/geth/nft_analyse/data/apeCoin/nftSwap.csv"
    pairAddress_csv="/geth/nft_analyse/data/swapDex/pairWithWeth.csv"
    event_claim_csv="/geth/nft_analyse/data/apeCoin/receipt_claim.csv"
    event_mint_csv="/geth/nft_analyse/data/apeCoin/receipt_mint.csv"
    event_redeem_csv="/geth/nft_analyse/data/apeCoin/receipt_redeem.csv"
    flashbotsTxMap={}
    txMap={}
    pairAddressMap={}

    # addFlashbots(flashbotsTxMap)
    # getPairAddress(pairAddressMap,pairAddress_csv)
    get_redeem_fromEvent(event_redeem_csv,flashbotsTxMap,txMap)
    get_claim_fromEvent(event_claim_csv,flashbotsTxMap,txMap)
    get_mint_fromEvent(event_mint_csv,flashbotsTxMap,txMap)
    ouputCsv(txMap,outputPath_csv)
    
    # get_reward_fromERC20(txMap,flashbotsTxMap,pairAddressMap,outputPath_dict,outputPath_csv)
    
if __name__ == '__main__':
    main()