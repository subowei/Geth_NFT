import numpy as np
import csv
import zipfile
import os
import json
import pickle

# blockNumber,timestamp,transactionHash,typeTraceAddress,from,to,fromIsContract,toIsContract,value,callingFunction,isError
def getInterTransactionFromXblock(resTxMap,flashbotsTxMap,valid_contract_address,path):
    print("getInterTransactionFromXblock")
    fileDir = "/internalTransaction/";

    files = [
		# "13000000to13249999_InternalTransaction",
		# "13250000to13499999_InternalTransaction",
		# "13500000to13749999_InternalTransaction",
		# "13750000to13999999_InternalTransaction",
		"14000000to14249999_InternalTransaction",
		"14250000to14499999_InternalTransaction",
		"14500000to14749999_InternalTransaction",
		"14750000to14999999_InternalTransaction"
    ];
    for file in files:
        print("file",file)
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        theCSV = theZIP.open(file+".csv");

        head = theCSV.readline();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            fromAddr=oneArray[4]
            toAddr=oneArray[5]
            
            try:
                _=flashbotsTxMap[transactionHash]
                if toAddr==valid_contract_address:
                    if transactionHash in resTxMap.keys():
                        value=resTxMap[transactionHash]
                    else:
                        value={}
                    value["internal"]=oneArray
                    resTxMap[transactionHash]=value
            except:
                pass    
            
            try:
                _=flashbotsTxMap[transactionHash]
                if fromAddr==valid_contract_address:
                    if transactionHash in resTxMap.keys():
                        value=resTxMap[transactionHash]
                    else:
                        value={}
                    value["callReward"]=oneArray[8]
                    resTxMap[transactionHash]=value
            except:
                pass    
                
                
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        ouputCsv(resTxMap,flashbotsTxMap,path,valid_contract_address)
            
# ['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 
# 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed', 
# 'callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas']
def getNormalTransactionFromXblock(resTxMap,flashbotsTxMap,valid_contract_address,path):
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
            toAddr=oneArray[4]
            
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0
                
            oneArray.append(str(position))
            # if to is kitty, add normal
            try:
                _=flashbotsTxMap[transactionHash]
                if toAddr==valid_contract_address:
                    if transactionHash in resTxMap.keys():
                        value=resTxMap[transactionHash]
                    else:
                        value={}
                    value["normal"]=oneArray
                    resTxMap[transactionHash]=value
            except:
                pass
            
            # if internal data exists, add normal
            try:
                _=resTxMap[transactionHash]["internal"]
                value=resTxMap[transactionHash]
                value["normal"]=oneArray
                resTxMap[transactionHash]=value
            except:
                pass
                    
            position+=1
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        ouputCsv(resTxMap,flashbotsTxMap,path,valid_contract_address)

def ouputCsv(txMap,flashbotsTxMap,path,valid_contract_address):
    f = open(path,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position","callingFunction_internal","callingFunction_normal","callReward","transactionFee","coinbase_transfer","total_miner_reward"])
    for key,value in txMap.items():
        callingFunction_internal="none"
        callingFunction_normal="none"
        transactionFee="none"
        blockNumber="none"
        position="none"
        timestamp="none"
        coinbase_transfer=flashbotsTxMap[key]["coinbase_transfer"]
        total_miner_reward=flashbotsTxMap[key]["total_miner_reward"]
        try:
            callingFunction_internal=value["internal"][9]
            blockNumber=value["internal"][0]
        except:
            pass
        
        try:
            callingFunction_normal=value["normal"][12]
            gasPrice=value["normal"][10]
            gasUsed=value["normal"][11]
            position=value["normal"][-1]
            transactionFee=str(int(gasPrice)*int(gasUsed))
            blockNumber=value["normal"][0]
            timestamp=value["normal"][1]
        except:
            pass
        
        try:
            callReward=value["callReward"]
        except:
            callReward="none"
        
        row=[key,blockNumber,timestamp,position,callingFunction_internal,callingFunction_normal,callReward,transactionFee,coinbase_transfer,total_miner_reward]
        writer.writerow(row)

def cryptoKitty():
    path="/geth/nft_analyse/data/kitty/giveBirth_14_15_v1.csv"
    valid_contract_address="0x06012c8cf97bead5deae237070f9587f8e7a266d"
    flashbotsTxMap={}
    resTxMap={}
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
        
    getInterTransactionFromXblock(resTxMap,flashbotsTxMap,valid_contract_address,path)
    getNormalTransactionFromXblock(resTxMap,flashbotsTxMap,valid_contract_address,path)
    
    
    
if __name__ == '__main__':
    cryptoKitty()