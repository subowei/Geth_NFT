import numpy as np
import csv
import zipfile
import os
import json
import pickle
            
# ['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 
# 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed', 
# 'callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas']
def getNormalTransactionFromXblock(resList,flashbotsTxMap,valid_contract_address,path):
    print("getNormalTransactionFromXblock")
    fileDir = "/normalTransaction/";

    files = [
		"13000000to13249999_BlockTransaction",
		"13250000to13499999_BlockTransaction",
		"13500000to13749999_BlockTransaction",
		"13750000to13999999_BlockTransaction",
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
        m=0
        i=0
        curBlockNumber=0
        position=0
        oneArray_first=[]
        oneArray_second=[]
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            toAddr=oneArray[4]
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0

            
            oneArray.append(str(position))
            
            if oneArray_first==[]:
                oneArray_first=oneArray
                
            if oneArray_second==[]:
                oneArray_second=oneArray
                
            if oneArray_first[12]=="0x091dbfd2" and oneArray_second[12]=="0x23165b75" \
            and oneArray_first[0]==oneArray_second[0] and int(oneArray_first[18])+1==int(oneArray_second[18]) \
            and oneArray_first[4]==valid_contract_address and oneArray_second[4]==valid_contract_address:
                try:
                    _=flashbotsTxMap[oneArray[2]]
                    _=flashbotsTxMap[oneArray_first[2]]
                    _=flashbotsTxMap[oneArray_second[2]]
                    
                    resList.append(oneArray_first)
                    resList.append(oneArray_second)
                    resList.append(oneArray)
                except:
                    pass
            
            oneArray_first=oneArray_second
            oneArray_second=oneArray
            position+=1
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        ouputCsv(resList,flashbotsTxMap,path)

# normalTransaction
# ['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 
# 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed', 
# 'callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas']
def ouputCsv(resList,flashbotsTxMap,path):
    f = open(path,'w')
    writer = csv.writer(f)
    writer.writerow(['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed','callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas','position','coinbase_transfer','total_miner_reward'])
    for row in resList:
        coinbase_transfer=flashbotsTxMap[row[2]]["coinbase_transfer"]
        total_miner_reward=flashbotsTxMap[row[2]]["total_miner_reward"]
        tempRow=row.copy()
        tempRow.append(coinbase_transfer)
        tempRow.append(total_miner_reward)
        writer.writerow(tempRow)
        
        
def cryptoPunks():
    path="/geth/nft_analyse/data/punk/sandwich_punk.csv"
    valid_contract_address="0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
    flashbotsTxMap={}
    resList=[]
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    getNormalTransactionFromXblock(resList,flashbotsTxMap,valid_contract_address,path)

        
if __name__ == '__main__':
    cryptoPunks()