import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

def addFlashbots(flashbotsTxMap):
    maxBlockNum=0
    file=open("/flashbots/all_blocks").read()
    all_blocks=json.loads(file)
    for block in all_blocks:
        transactions=block["transactions"]
        block_number=int(block["block_number"])
        if block_number<13000000 or block_number>15000000:
            continue
        
        if block_number>maxBlockNum:
            maxBlockNum=block_number
        
        for transaction in transactions:
            transaction_hash=transaction["transaction_hash"]
            flashbotsTxMap[transaction_hash]=transaction
            
            
    print("maxBlockNum",maxBlockNum)
            
            
    with open("/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "wb") as tf:
        pickle.dump(flashbotsTxMap,tf)


def main():
    flashbotsTxMap={}
    addFlashbots(flashbotsTxMap)
    
    
if __name__ == '__main__':
    main()