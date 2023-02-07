import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

# 筛选具有evilReveal漏洞的数据
def getTokenList(path):
    resList=[]
    f=open(path)
    line=f.readline()
    while line:
        temp=line.strip('\n')
        if temp!='':
            resList.append(temp.lower())
        line=f.readline()
    f.close()
    return resList

def output_evilReveal(tokenList,inputCsv,outputCsv):
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        # reader = csv.reader((line.replace('\0', '') for line in fOld), delimiter=",")
        header_row=next(reader)
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%1000==0:
                print(i)
            i+=1
            tokenAddress=row[1]
            if tokenAddress in tokenList:
                writer.writerow(row)

def addEvilReveal():
    inputCsv="/geth/nft_analyse/data/nftflow/tokenMapERC721ResSorted_withTxStatus_flashbots_duplicateRemoval.csv"
    outputCsv="/geth/nft_analyse/data/evilreveal/tokenMapERC721ResSorted_withTxStatus_flashbots_duplicateRemoval_evilReveal.csv"
    tokenListTxt="/geth/nft_analyse/data/evilreveal/evilReveal-main/Dataset/eth.txt"
    tokenList=getTokenList(tokenListTxt)
    output_evilReveal(tokenList,inputCsv,outputCsv)
    
    
if __name__ == '__main__':
    addEvilReveal()