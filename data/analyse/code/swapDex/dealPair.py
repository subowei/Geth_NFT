import numpy as np
import csv
import zipfile
import os
import json
import pickle
import os


def ouputCsv(outputCsv,pairMap):
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["pairAddress","tokenAddress0","tokenAddress1"])
    for key,value in pairMap.items():
        row=[key,value["tokenAddress0"],value["tokenAddress1"]]
        writer.writerow(row)


def getPairWithWeth(outputCsv,pairMap):
    weth="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
    dir="/swap/"
    fileNameList=["UniswapV2_PairInfo.csv","UniswapV3_13to14_PoolInfo.csv","SushiSwap_PairInfo.csv"]
    for fileName in fileNameList:
        filePath=dir+fileName
        with open(filePath,'r', encoding="UTF8") as f:
            reader = csv.reader(f)
            header_row=next(reader)
            i=0
            for row in reader:
                if i%10000==0:
                    print(i)
                i+=1
                pairAddress=row[0]
                tokenAddress0=row[1]
                tokenAddress1=row[2]
                if tokenAddress0!=weth and tokenAddress1!=weth:
                    continue
                
                if pairAddress not in pairMap.keys():
                    pairMap_value={}
                else:
                    pairMap_value=pairMap[pairAddress]
                pairMap_value["tokenAddress0"]=tokenAddress0
                pairMap_value["tokenAddress1"]=tokenAddress1
                pairMap[pairAddress]=pairMap_value
                
        ouputCsv(outputCsv,pairMap)


# only get pair<weht,otherToken> in uniswap and sushiswap
def main():
    pairMap={}
    outputCsv="/geth/nft_analyse/data/swapDex/pairWithWeth.csv"
    getPairWithWeth(outputCsv,pairMap)
    

if __name__ == '__main__':
    main()