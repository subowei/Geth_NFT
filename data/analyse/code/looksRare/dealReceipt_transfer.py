
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

# deal prefix "0"
def deal_topics_item(tempStr):
    tempStr="0x"+tempStr[26:]
    return tempStr
    
# convert 10 from 16
def deal_data_item(tempStr):
    # delete prefix "0"
    tempStr=tempStr.lstrip("0x")
    if tempStr=="":
        tempStr="0"
        return tempStr
    convertion = int(tempStr, 16)
    return str(convertion)

def dealReceiptFile(result,outputList,txMap):
    for receipt in result:
        logs=receipt["logs"]
        for log in logs:
            address=log["address"]
            topics=log["topics"]
            data=log["data"]
            transactionHash=log["transactionHash"]
            blockNumber=deal_data_item(log["blockNumber"])
            try:
                _=txMap[transactionHash]
                if len(topics)==3 and topics[0]==event_select and address==target_address:                
                    fromAddr=deal_topics_item(topics[1])
                    toAddr=deal_topics_item(topics[2])
                    
                    wad=deal_data_item(data)
                    tempList=[transactionHash,blockNumber,fromAddr,toAddr,wad]
                    outputList.append(tempList)
            except:
                pass


def dealZipFile(fileDir,outputCsv,txMap):
    outputList=[]
    
    # 1. get all receipt file name
    fileNameList=[]
    for fileName in os.listdir(fileDir):
        if ".zip" in fileName:
            fileNameList.append(fileName.split(".zip")[0])
    
    # 2. unzip and deal
    for file in fileNameList:
        print(file)
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r')
        for receiptFileName in theZIP.namelist():
            if "txt" not in receiptFileName:
                continue
            theTxt = theZIP.open(receiptFileName,"r")
            result=json.loads(theTxt.read().decode('UTF-8'))["result"]
            if len(result)!=0:
                dealReceiptFile(result,outputList,txMap)
                
        ouputCsv(outputList,outputCsv)
        
    print("finish")

def ouputCsv(outputList,outputCsv):
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","fromAddr","toAddr","wad"])
    for row in outputList:
            writer.writerow(row)
            
            
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

def main():
    txMap={}
    event_takerBid_csv="/geth/nft_analyse/data/looksRare/receipt_takerBid.csv"
    event_takerAsk_csv="/geth/nft_analyse/data/looksRare/receipt_takerAsk.csv"
    get_takerBid_fromEvent(event_takerBid_csv,txMap)
    get_takerAsk_fromEvent(event_takerAsk_csv,txMap)
    
    outputCsv="/geth/nft_analyse/data/looksRare/receipt_transfer.csv"
    dealZipFile(receipt_path,outputCsv,txMap)

target_address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower()
receipt_path="/receipt2/"
event_select="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


if __name__ == '__main__':
    main()