
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
    tempStr=tempStr.lstrip("0")
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
            topics=log["topics"]
            data=log["data"]
            transactionHash=log["transactionHash"]
            if len(topics)==4 and topics[0]==OrdersMatched_select:
                maker=deal_topics_item(topics[1])
                taker=deal_topics_item(topics[2])
                price=deal_data_item(data[-64:])
                tempList=[transactionHash,maker,taker,price]
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
    writer.writerow(["transactionHash","maker","taker","price"])
    for row in outputList:
            writer.writerow(row)

def main():
    outputCsv="/geth/nft_analyse/data/opensea/receipt_14_15.csv"
    dealZipFile(receipt_path,outputCsv)


receipt_path="/receipt2/"
OrdersMatched_select="0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"


if __name__ == '__main__':
    main()