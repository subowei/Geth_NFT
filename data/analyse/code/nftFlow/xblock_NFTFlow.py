import numpy as np
import csv
import zipfile
import os
import json
import pickle
import pandas as pd

def addFlashbots(flashbotsTxMap):
    file=open("/all_blocks").read()
    all_blocks=json.loads(file)
    for block in all_blocks:
        transactions=block["transactions"]
        for transaction in transactions:
            transaction_hash=transaction["transaction_hash"]
            flashbotsTxMap[transaction_hash]=1
            
def dealGethRes(flashbotsTxMap,inputCsv,outputCsv):
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash_0=row[7].lower()
            transactionHash_1=row[11].lower()
            
            if transactionHash_0 in flashbotsTxMap.keys() or transactionHash_1 in flashbotsTxMap.keys():
                writer.writerow(row)
                
# blockNumber,timestamp,transactionHash,from,to,toCreate,fromIsContract,toIsContract,value,gasLimit,gasPrice,
# gasUsed,callingFunction,isError,eip2718type,baseFeePerGas,maxFeePerGas,maxPriorityFeePerGas
def getTxStatus(txMap,txDataMap,outputDict):
    print("getNormalTransactionFromXblock")
    fileDir = "/normalTransaction/";

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
            blockNumber=oneArray[0]
            timestamp=oneArray[1]
            transactionHash=oneArray[2]
            fromAddr=oneArray[3]
            toAddr=oneArray[4]
            gasPrice=oneArray[10]
            gasUsed=oneArray[11]
            isError=oneArray[13]
            transactionFee=str(int(gasPrice)*int(gasUsed))
            try:
                _=txMap[transactionHash]
                txDataMap[transactionHash]={"transactionFee":transactionFee,"isError":isError,"from":fromAddr,"to":toAddr,"timestamp":timestamp}
            except:
                pass
            
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        with open(outputDict, "wb") as tf:
            pickle.dump(txDataMap,tf)
        
# blockNum,tokenAddress,tokenId,startAddr,tokenIdOwnerAddrOriginal,tokenIdOwnerAddrEdited,endAddr_0,transactionHash_0,positionOriginal_0,positionEdited_0,
# endAddr_1,transactionHash_1,positionOriginal_1,positionEdited_1
def addStatus(txDataMap,inputCsv,outputCsv):
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        # header_row.insert(8,"isError_0")
        # header_row.insert(9,"transactionFee_0")
        # header_row.insert(13,"isError_1")
        # header_row.insert(14,"transactionFee_1")
        header_row.append("timestamp")
        header_row.append("isError_0")
        header_row.append("transactionFee_0")
        header_row.append("from_0")
        header_row.append("to_0")
        header_row.append("isError_1")
        header_row.append("transactionFee_1")
        header_row.append("from_1")
        header_row.append("to_1")
        
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash_0=row[7].lower()
            transactionHash_1=row[11].lower()
            isError_0="none"
            isError_1="none"
            transactionFee_0="none"
            transactionFee_1="none"
            from_0="none"
            to_0="none"
            blockNumber="none"
            from_1="none"
            to_1="none"
            
            try:
                isError_0=txDataMap[transactionHash_0]["isError"]
                transactionFee_0=txDataMap[transactionHash_0]["transactionFee"]
                from_0=txDataMap[transactionHash_0]["from"]
                to_0=txDataMap[transactionHash_0]["to"]
                timestamp=txDataMap[transactionHash_0]["timestamp"]

                isError_1=txDataMap[transactionHash_1]["isError"]
                transactionFee_1=txDataMap[transactionHash_1]["transactionFee"]
                from_1=txDataMap[transactionHash_1]["from"]
                to_1=txDataMap[transactionHash_1]["to"]
            except:
                pass
            
            # row.insert(8,isError_0)
            # row.insert(9,transactionFee_0)
            # row.insert(13,isError_1)
            # row.insert(14,transactionFee_1)
            row.append(timestamp)
            row.append(isError_0)
            row.append(transactionFee_0)
            row.append(from_0)
            row.append(to_0)
            row.append(isError_1)
            row.append(transactionFee_1)
            row.append(from_1)
            row.append(to_1)
            writer.writerow(row)
            

def getTxs_fromROG(txMap,inputCsv):
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash_0=row[7].lower()
            transactionHash_1=row[11].lower()
            
            txMap[transactionHash_0]=1
            txMap[transactionHash_1]=1        
            
    
# 给rog的数据添加交易状态
def addStatusToRog():
    inputCsv="/geth/nft_analyse/data/csv/erc721/tokenMapERC721ResSorted.csv"
    outputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus.csv"
    outputDict="/geth/nft_analyse/data/nftflow/txStatus_13to14.map"
    txMap={}
    txDataMap={}
    
    getTxs_fromROG(txMap,inputCsv)
    getTxStatus(txMap,txDataMap,outputDict)
    # with open(outputDict, "rb") as tf:
    #     txDataMap=pickle.load(tf)
    addStatus(txDataMap,inputCsv,outputCsv)
    
    
def addFlashbotsToRog():
    inputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus.csv"
    outputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus_flashbots.csv"
    flashbotsTxMap={}
    addFlashbots(flashbotsTxMap)
    dealGethRes(flashbotsTxMap,inputCsv,outputCsv)
    
# blockNum,tokenAddress,tokenId,startAddr,tokenIdOwnerAddrOriginal,
# tokenIdOwnerAddrEdited,endAddr_0,transactionHash_0,positionOriginal_0,
# positionEdited_0,endAddr_1,transactionHash_1,positionOriginal_1,positionEdited_1,
# blockNumber,isError_0,transactionFee_0,from_0,to_0,isError_1,transactionFee_1,from_1,to_1

def duplicateRemoval():
    inputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus_flashbots.csv"
    outputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus_flashbots_duplicateRemoval.csv"

    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    
    df=pd.read_csv(inputCsv)
    header_row=list(df.columns)
    writer.writerow(header_row)
    
    i=0
    for index,row in df.iterrows():
        if i%10000==0:
            print(i)
        i+=1
        if row["isError_1"]!="none":
            # if int(row["positionOriginal_0"]) < int(row["positionEdited_0"]) and int(row["positionOriginal_0"])<int(row["positionOriginal_1"]) and int(row["positionEdited_0"])>int(row["positionEdited_1"]):
            if int(row["positionOriginal_0"])<int(row["positionOriginal_1"]) and int(row["positionEdited_0"])>int(row["positionEdited_1"]):

                writer.writerow(row.values)
        else:
            writer.writerow(row.values)

            
            
def dealGethRes_1(flashbotsTxMap,inputCsv,outputCsv):
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("coinbase_transfer_0")
        header_row.append("coinbase_transfer_1")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash_0=row[7].lower()
            transactionHash_1=row[11].lower()
            try:
                coinbase_transfer_0=flashbotsTxMap[transactionHash_0]["coinbase_transfer"]
            except:
                coinbase_transfer_0="none"
                
            try:
                coinbase_transfer_1=flashbotsTxMap[transactionHash_1]["coinbase_transfer"]
            except:
                coinbase_transfer_1="none"
            
            row.append(coinbase_transfer_0)
            row.append(coinbase_transfer_1)
            
            if transactionHash_0 in flashbotsTxMap.keys() or transactionHash_1 in flashbotsTxMap.keys():
                writer.writerow(row)
                
def addFlashbots_minerData():
    inputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus_flashbots_duplicateRemoval.csv"
    outputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus_flashbots_duplicateRemoval_v1.csv"
    flashbotsTxMap={}
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    dealGethRes_1(flashbotsTxMap,inputCsv,outputCsv)

def main():

    # addStatusToRog()
    
    # addFlashbotsToRog()
    
    # # duplicate removal
    # duplicateRemoval()
    
    addFlashbots_minerData()
    
if __name__ == '__main__':
    main()