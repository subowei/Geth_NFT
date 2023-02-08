
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
            if len(topics)==1 and topics[0]==event_select:
                tempList=[transactionHash]
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
    writer.writerow(["transactionHash"])
    for row in outputList:
            writer.writerow(row)

def main():
    outputCsv="/geth/nft_analyse/data/apeCoin/receipt_mint.csv"
    dealZipFile(receipt_path,outputCsv)


targetTokenAddress="0x025C6da5BD0e6A5dd1350fda9e3B6a614B205a1F".lower()
receipt_path="/receipt2/"
event_select="0x1f72ad2a14447fa756b6f5aca53504645af79813493aca2d906b69e4aaeb9492"


if __name__ == '__main__':
    main()