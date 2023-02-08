
import numpy as np
import csv
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

# only deal OrdersMatched event in opensea
def dealReceiptFile(result,outputList):
    for receipt in result:
        logs=receipt["logs"]
        for log in logs:
            address=log["address"]
            topics=log["topics"]
            data=log["data"]
            transactionHash=log["transactionHash"]
            blockNumber=deal_data_item(log["blockNumber"])
            if len(topics)==4 and topics[0]==event_select:
                target=deal_topics_item(topics[1])
                initiator=deal_topics_item(topics[2])
                asset=deal_topics_item(topics[3])
                
                amount=deal_data_item(data[0:66])
                premium=deal_data_item(data[66:130])
                referralCode=deal_data_item(data[130:194])
                
                tempList=[transactionHash,blockNumber,target,initiator,asset,amount,premium,amount,referralCode]
                outputList.append(tempList)


def dealZipFile(fileDir,outputCsv):
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
                dealReceiptFile(result,outputList)
                
        ouputCsv(outputList,outputCsv)
        
    print("finish")

def ouputCsv(outputList,outputCsv):
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","target","initiator","asset","amount","premium","amount","referralCode"])
    for row in outputList:
            writer.writerow(row)

def main():
    outputCsv="/geth/nft_analyse/data/aave/receipt_flashLoan.csv"
    dealZipFile(receipt_path,outputCsv)


receipt_path="/receipt/"
event_select="0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac"


if __name__ == '__main__':
    main()