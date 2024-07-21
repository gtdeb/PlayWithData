import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np
import json
from entity_model import Db
import jwt
from jwt import encode as jwt_encode
from collections import OrderedDict

# Secret key for signing the token
SECRET_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzg1MDg1NSwiaWF0IjoxNzE3ODUwODU1fQ.ZuUT0LHHEUwduX0JPig7xKELWCGZdnPm0uEk7jcDlXY"

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client["mydatabase"]
users_collection = db["users"]

def generate_token(email: str) -> str:
    payload = {"email": email}
    token = jwt_encode(payload, SECRET_KEY, algorithm="HS256")
    return token

def getDatabaseCollection_JSON():
    dict_df=dict()
    collections = db.list_collection_names()
    
    if len(collections) > 0:
        df = pd.DataFrame(collections, columns=['name'])
        dict_df = df.to_dict("records")
        for item in dict_df:
            item['id'] = item['name']+'-collection-mydatabase-mongo'
            item['type'] = 'collection'

    dbCollections = {'id': 'mydatabase-mongo', 'name': 'mydatabase', 'type': 'mongodb', 'children': dict_df}
    return dbCollections

def maxRowId_collection(req: Db):
    my_collection=db[req.table]
    res=my_collection.find_one(sort=[("id", pymongo.DESCENDING)])
    print("maxRowId_collection: ", res)
    return res['id']
    
def getDataFromCollection_JSON(req: Db, fetchCount: int):
    doc_list = list()
    i=1;
    my_collection=db[req.table]
    cursor=my_collection.find()
    for item in cursor:
        print('item: ', item)
        item.pop('_id')
        if req.table == "users":
            item.pop('token')
        updict={'id': item['id']}
        item.pop('id')
        updict.update(item)
        doc_list.append(updict)
        if i >= fetchCount:
            break
        i+=1
    
    maxrows=my_collection.count_documents({})
    print("Total documents fetched: {}".format(len(doc_list)))
    return(doc_list, {'maxrows': maxrows})
    
def insertCollection_JSON(req, addBuf):
    my_collection=db[req.table]
    for coln in addBuf:
        if req.table == "users":
            token = generate_token(coln['email'])
            coln['token']=token
        coln['id']=str(coln['id'])

    status=my_collection.insert_many(addBuf)
    return {'message': status}

def updateCollection_JSON(req, dirtyBuf):
    print(dirtyBuf)
    my_collection=db[req.table]
    for coln in dirtyBuf:
        ids=coln['id']
        coln.pop('id')
        print(coln, ids)
        status=my_collection.update_many( 
                {"id": { "$eq": ids } }, 
                { 
                    "$set": coln 
                }
            )
    return {'message': status}  

def deleteCollection_JSON(req, delBuf):
    print(delBuf)
    my_collection=db[req.table]
    query={}
    for ids in delBuf:
        print('ids: ',ids)
        query['id']=ids
        myquery=query
        print(myquery)
        my_collection.delete_one(myquery)
    return True

def importdatamongo_collection(selectedFile, collectionName, vColumns):
    collist=db.list_collection_names()
    #Delete collection if exists
    if collectionName in collist:
        my_collection=db[collectionName]
        print("collectionName exists: ", collectionName)
        my_collection.drop()
    my_collection = db[collectionName]

    #load data from csv into data frame and covert that to dict
    selectedCols=[] #Prepare a list of selected columns
    for vcol in vColumns:
        if vcol['field'] != 'id':
            selectedCols.append(vcol['field'])
    absFilePath="./datasets/" + selectedFile
    df=pd.read_csv(absFilePath, usecols=selectedCols) #nrows=200
    df = df.replace(np.nan, "Unknown")
    data_list=[]
    for index in range(0,len(df)):
        row=df.iloc[index].to_dict()
        ordered_row = OrderedDict([('id', str(index + 1))])
        ordered_row.update(row)
        data_list.append(ordered_row)
    
    #Now insert data into the collection
    status=my_collection.insert_many(data_list)
    rows_inserted=my_collection.count_documents({})
    print('rows_inserted: ', rows_inserted)
    return str(rows_inserted)+" of "+str(len(df))+" records inserted"

def dropcollection(selectedNode):
    my_collection=db[selectedNode['name']]
    dropstatus=1
    
    try:
        my_collection.drop()
    except:
        dropstatus=0
    finally:
        return dropstatus
