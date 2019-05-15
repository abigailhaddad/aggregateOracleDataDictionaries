# -*- coding: utf-8 -*-
"""
This takes an Oracle-output data dictionary from Excel, aggregates the tabs, and then cleans/aggregates by so that when there's unit-level information
that's in multiple rows, it's now all in the same row of the final DF
"""

### can we run things here??

import os
import pandas as pd
import numpy as np

# open an excel file, get the tabs
def openFileGetTabs(fileName):
    workBook = pd.ExcelFile(fileName)
    return(workBook.sheet_names)

# for each tab, read it in, skipping the first line
def getTableFromExcel(fileName, sheetName):
    df=pd.read_excel(fileName, sheet_name=sheetName, header=1)
    return(df)

#put all of these into a dictionary    
def getAllTablesFromExcel(fileName, sheetNames):
    listOfDFs=[getTableFromExcel(fileName, sheetName) for sheetName in sheetNames]
    dictionaryOfDFs=dict(zip(sheetNames, listOfDFs))
    return(dictionaryOfDFs)
    
# do cleaning within each table
def cleanADataFrame(tabName, myDF):
    # forward fill the #
    myDF['#']=myDF['#'].fillna(method='ffill')
    #put in the tab name
    myDF['tabName']=tabName
    return(myDF)

def cleanAllDataFrames(dictionaryOfDFs):
    listOfCleanDFs=[cleanADataFrame(key, value) for key, value in dictionaryOfDFs.items()]
    return(listOfCleanDFs)
    
# concatenate each of the data frames 
def aggregateDFs(listOfCleanDFs):
    bigDF = pd.concat(listOfCleanDFs)
    return(bigDF)

# get the uniques, drop the missings, join it into a string

def takeColumnsGroupByGetText(partDF):
    # group text by the # for each separate column
    columnWeWant=partDF.columns[1]
    dfIntermediate=partDF.groupby(['indexColumn'])[columnWeWant].apply(list)
    df=dfIntermediate.apply(cleanList)
    return(df)
    
def cleanList(myList):
    myNonBlankList = [i for i in myList if str(i) != 'nan']
    myUniqueList=list(set(myNonBlankList))
    myString= ','.join(map(str, myUniqueList))
    return(myString)
    
def aggregateColumnsForCleanDF(allDF):
    allDF['indexColumn']=allDF['#'].astype(str)+"  " +allDF['tabName']
    newDF=pd.DataFrame(index=allDF['indexColumn'].unique())
    columnsWeWant=list(allDF.columns)
    columnsWeWant.remove("#")
    columnsWeWant.remove('indexColumn')
    mySerieses=[takeColumnsGroupByGetText(allDF[['indexColumn',column]])for column in columnsWeWant]
    newDF = pd.concat(mySerieses, axis=1, keys=allDF['indexColumn'].unique())
    newDF.columns=columnsWeWant
    return(newDF)

def takeFileNameOutPutFinalDF(fileName):    
    sheetNames=openFileGetTabs(fileName)
    dictionaryOfDFs=getAllTablesFromExcel(fileName, sheetNames)
    listOfCleanDFs=cleanAllDataFrames(dictionaryOfDFs)
    allDF=aggregateDFs(listOfCleanDFs)
    cleanFinalDF=aggregateColumnsForCleanDF(allDF)
    cleanFinalDF.replace(r'^\s*$', np.nan, regex=True, inplace = True)
    return(cleanFinalDF)
    
myDirectory=r'H:\_MyComputer\Documents\Python Scripts\dataDictionary'
os.chdir(myDirectory)
fileName="DITPR Data Dictionary 2019 01.xlsx"
finalDF=takeFileNameOutPutFinalDF(fileName)

