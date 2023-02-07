import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

def get_takerBid_fromEvent(event_csv,txMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            
            try:
                txMap_value=txMap[transactionHash]
            except:
                txMap_value={}

            txMap_value["takerBid"]=row
            txMap[transactionHash]=txMap_value
            
            
def get_takerAsk_fromEvent(event_csv,txMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            
            try:
                txMap_value=txMap[transactionHash]
            except:
                txMap_value={}

            txMap_value["takerAsk"]=row
            txMap[transactionHash]=txMap_value
            
def get_transfer_fromEvent(event_csv,txMap):
    with open(event_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash=row[0]
            
            try:
                txMap_value=txMap[transactionHash]
            except:
                txMap_value={}

            txMap_value["transfer"]=row
            txMap[transactionHash]=txMap_value
            
            
def getNormalTransactionFromXblock(txMap,flashbotsTxMap,outputPath_csv):
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
            timestamp=oneArray[1]
            
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0
                
            # oneArray.append(str(position))
            
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
            
        ouputCsv(txMap,flashbotsTxMap,outputPath_csv)
            
# transactionHash,blockNumber,fromAddr,toAddr,currency,collection,tokenId,amount,price
def ouputCsv(txMap,flashbotsTxMap,outputPath_csv):
    f = open(outputPath_csv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position","transactionFee","coinbase_transfer","reward"])
    for key,value in txMap.items():
        if "takerAsk" not in value.keys() or "takerBid" not in value.keys() or "transfer" not in value.keys():
            continue
        
        try:
            coinbase_transfer=flashbotsTxMap[key]["coinbase_transfer"]
            blockNumber=value["blockNumber"]
            position=value["position"]
            transactionFee=value["transactionFee"]
            timestamp=value["timestamp"]
        except:
            continue
        
        takerAsk=value["takerAsk"]
        takerBid=value["takerBid"]
        transfer=value["transfer"]
        
        # buy and sell
        if takerBid[3]!=takerAsk[2] or takerBid[4]!=takerAsk[4] or takerBid[5]!=takerAsk[5] or takerBid[6]!=takerAsk[6]:
            continue
        
        if transfer[2]!=takerAsk[3] or transfer[3]!=takerAsk[2]:
            continue
        
        reward=(int(transfer[4])-int(takerBid[8])) * int(takerBid[7])

        
        row=[key,blockNumber,timestamp,position,transactionFee,coinbase_transfer,reward]
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
    txMap={}
    flashbotsTxMap={}
    event_takerBid_csv="/geth/nft_analyse/data/looksRare/receipt_takerBid.csv"
    event_takerAsk_csv="/geth/nft_analyse/data/looksRare/receipt_takerAsk.csv"
    event_transfer="/geth/nft_analyse/data/looksRare/receipt_transfer.csv"
    outputPath_csv="/geth/nft_analyse/data/looksRare/sigleArbitrage.csv"
    
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    get_takerBid_fromEvent(event_takerBid_csv,txMap)
    get_takerAsk_fromEvent(event_takerAsk_csv,txMap)
    get_transfer_fromEvent(event_transfer,txMap)
    getNormalTransactionFromXblock(txMap,flashbotsTxMap,outputPath_csv)
    # ouputCsv(txMap,outputPath_csv)
        
if __name__ == '__main__':
    main()