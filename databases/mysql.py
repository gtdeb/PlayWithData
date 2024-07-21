import logging
import mysql.connector
import pandas as pd
import numpy as np
import json
from entity_model import Db

logging.basicConfig(level=logging.INFO)

def connectToDb():
    db = mysql.connector.connect(
        host = "localhost",
        database = "mysqldb",
        user = "scott",
        password = "tiger",
        auth_plugin='mysql_native_password'
    )
    return db

def closeConnection(db, cursor):
    if db.is_connected():
        cursor.close()
        db.close()

def createTable(cursor):
    tabstr="""CREATE TABLE SG_DATASET(ID INT PRIMARY KEY AUTO_INCREMENT,
              Name VARCHAR(200),
              Price FLOAT(5,2),
              Release_date VARCHAR(25),
              Genres VARCHAR(500),
              About_the_game TEXT)"""
    
    cursor.execute("DROP TABLE IF EXISTS SG_DATASET")
    cursor.execute(tabstr)
    
def loadToDb(db, cursor):
    df=pd.read_csv("datasets\steam_game_dataset_short.csv", 
                    usecols=["Name", "Price", "Release_date", "Genres", "About_the_game"],
                    nrows=1999)
    df = df.replace(np.nan, "Unknown")
    #print(df["Price"])
    #createTable(cursor)
    
    try:
        cursor.execute("TRUNCATE TABLE SG_DATASET")
    except:
        print("Truncate error...")
    else:
        print("SG_DATASET truncated") 
        
    try:
        #Inser values into the table
        val = []
        i=1
        instr="INSERT INTO SG_DATASET(Name,Price,Release_Date,Genres,About_the_game) VALUES(%s, %s, %s, %s, %s)"
        for indx in df.index:
            val.append((df["Name"][indx], df["Price"][indx], df["Release_date"][indx], df["Genres"][indx], df["About_the_game"][indx]))
            if i%5 == 0:
                cursor.executemany(instr,val)
                val = []
            i += 1
        if(len(val)):
            cursor.executemany(instr,val)
            val = []
        db.commit()
        print("{} rows inserted...".format(i-1))
    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))
    except:
        print(val)
    finally:
        if db.is_connected():
            cursor.close()
            db.close()
def getCondQuery():
    print("Enter query condition: ")
    cquery = input()
    return cquery

def dumpToJSONFile(dict_df):
    try:
        with open("result.json", "w") as jp:
            json.dump(dict_df, jp, indent=4, sort_keys=True)
    except:
        print("Can't dump to json")
    else:
        print("Result dumped successfully into \'result.json\'")
    finally:
        jp.close()

def tableCreateQuery(tableName, query):
    db = connectToDb()
    cursor=db.cursor()
    response=""
    
    prequery="DROP TABLE IF EXISTS " + tableName
    try:
        cursor.execute(prequery)
    except:
        print("Fail to drop table")
    finally:
        try:
            cursor.execute(query)
        except:
            response=0
            print("Fail to create table")
        finally:
            closeConnection(db, cursor)
            response=1
            print("Table creation successful")
    return response;

def queryGeneral(query):
    db = connectToDb()
    cursor=db.cursor()
    resultset = None
    
    try:
        cursor.execute(query)
        resultset=cursor.fetchall()
    except:
        print("Fail to fetch result")
    finally:
        closeConnection(db, cursor)
        print("Total record/s fetched: {}".format(len(resultset)))
    return resultset;

def insertQuery(query, values):
    db = connectToDb()
    cursor=db.cursor()
    resultset = 0
    
    try:
        if len(values) > 1:
            cursor.executemany(query, values)
        else:
            cursor.execute(query, values)
        resultset=cursor.rowcount
        #print("insert result: ",resultset)
        db.commit()
    except:
        #print("Fail to insert")
        pass
    finally:
        closeConnection(db, cursor)
        #print("Total record/s inserted: {}".format(resultset))
        return resultset;

def updateQuery(query):
    db = connectToDb()
    cursor=db.cursor()
    resultset = None
    
    try:
        cursor.execute(query)
        resultset=cursor.rowcount
        print("update result: ",resultset)
        db.commit()
    except:
        print("Fail to update")
    finally:
        closeConnection(db, cursor)
        print("Total record/s updated: {}".format(resultset))
        return resultset;

def deleteQuery(query):
    db = connectToDb()
    cursor=db.cursor()
    resultset = None
    
    try:
        cursor.execute(query)
        resultset=cursor.rowcount
        print("delete result: ",resultset)
        db.commit()
    except:
        print("Fail to delete")
    finally:
        closeConnection(db, cursor)
        print("Total record/s deleted: {}".format(resultset))
        return resultset;

def dropQuery(query):
    db = connectToDb()
    cursor=db.cursor()
    dropstatus=1
    
    try:
        cursor.execute(query)
    except:
        dropstatus=0
    finally:
        closeConnection(db, cursor)
        return dropstatus

def queryGreaterThanPriceDisplay(db, cursor):
    condQuery = getCondQuery()
    query="SELECT NAME, PRICE, Release_date, GENRES FROM SG_DATASET WHERE " + condQuery
    
    resultset=queryGeneral(query)
    if len(resultset) > 0:
        df = pd.DataFrame(resultset, columns=["Name", "Price", "Release_date", "Genres"])
        dict_df = df.to_dict("records")
        #print(dict_df)
        dumpToJSONFile(dict_df)    
    #return len(resultset)

def getDatabaseTable_JSON():
    query="SELECT table_name FROM information_schema.tables WHERE table_schema = 'mysqldb'"
    dict_df = dict()
    
    resultset=queryGeneral(query)
    if len(resultset) > 0:
        df = pd.DataFrame(resultset, columns=['name'])
        dict_df = df.to_dict("records")
        for item in dict_df:
            item['id'] = item['name']+'-table-mysqldb-mysql'
            item['type'] = 'table'
            
    dbTables = {'id': 'mysqldb-mysql', 'name': 'mysqldb', 'type': 'mysqldb', 'children': dict_df}
    return dbTables
    
def getDataFromTable_JSON(req: Db, fetchCount: int):
    #Fetch columns first
    query= "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + req.table + "';"
    resultset=queryGeneral(query)
    cols=[]
    for colt in resultset:
        col = colt[0]
        if col not in 'ID': #About_the_game
            cols.append(col)
    cols.insert(0,'id');
    #print(cols)
    
    query="SELECT COUNT(*) FROM " + req.table + ";"
    resultset=queryGeneral(query)
    maxrows=(resultset[0])[0];
    #print(numrows)
    fetchCount=(fetchCount if fetchCount <= maxrows else maxrows)
    
    query="SELECT " + ','.join(cols) + " FROM " + req.table + " LIMIT " + str(fetchCount) + ";"
    print(query)
    dict_df = dict()
    
    resultset=queryGeneral(query)
    if len(resultset) > 0:
        df = pd.DataFrame(resultset, columns=cols)
        dict_df = df.to_dict("records")
    
    #logging.info(f"Table data fetched: {dict_df}")
    return(dict_df, {'maxrows': maxrows})

def maxRowId_table(req: Db):
    query="SELECT max(id) FROM " + req.table
    print(query)
    resultset=queryGeneral(query)
    return (resultset[0])[0]

def insertTable_JSON(req, addBuf):
    query=""
    insertCount=0
    print(addBuf)
    
    for aBuf in addBuf:
        keys=aBuf.keys()
        vals=aBuf.values()
        print('Keys: ', keys)
        print('Vals: ', vals)
        i=1
        cols=str()
        for key in keys:
            if key != 'id':
                if i == len(keys):
                    cols += str(key)
                else:
                    cols += str(key) + ","
            i +=1
        
        i=1
        values=[]
        for val in vals:
            if i > 1:
                _str=str(val)
                values.append(_str)
            i +=1
        print('values: ',values)
        spl=str()
        for i in range(len(keys)-1):
            if i == len(keys)-2:
                spl +='%s'
            else:    
                spl +='%s,'
        print(spl)
        query="INSERT INTO " + req.table + "(" + str(cols) + ") VALUES(" + str(spl) + ")"
        print(query)
        print(values)
        insertCount += insertQuery(query, values)
    return {"message": insertCount}


def updateTable_JSON(req, dirtyBuf):
    query=""
    updateCount=0
    for dBuf in dirtyBuf:
        keys=dBuf.keys()
        i=1
        kid=""
        #prepare update query
        query="UPDATE " + req.table + " SET "
        for key in keys:
            if key!='id':
                if i == len(keys):
                    query += key + "='" + str(dBuf[key]) + "'"
                else:
                    query += key + "='" + str(dBuf[key]) + "', "
            else:
                kid=key
            i +=1
        query += " WHERE " + kid + "=" + str(dBuf[kid])
        #print(query)
        updateCount += updateQuery(query)
    return {"message": updateCount}

def deleteTable_JSON(req, delBuf):
    query=""
    delCount=0
    print(delBuf)
    
    for _id in delBuf:
        #prepare delete query
        query="DELETE FROM " + req.table + " WHERE id=" + str(_id)
        #print(query)
        delCount += deleteQuery(query)
    return {"message": delCount}

def importdatamysql_table(selectedFile, tableName, vColumns, schemaData):
    query="CREATE TABLE " + tableName + "(ID INT PRIMARY KEY AUTO_INCREMENT,"
    length=len(schemaData)
    index=0
    inserColList=[]
    insertCount=0
    for vcol in schemaData:
        if vcol['columnName'] != 'id':
            vcol['columnName']="_".join(vcol['columnName'].replace('.', '').split())
            if index < length-1:
                query+=vcol['columnName'] + " " + vcol['dataType'] + ","
            else:
                query+=vcol['columnName'] + " " + vcol['dataType'] + ")"
            inserColList.append(vcol['columnName'])
        index+=1

    print(query)
    if(tableCreateQuery(tableName, query)): #Let's create the table
        #print(selectedFile)
        selectedCols=[] #Prepare a list of selected columns
        for vcol in vColumns:
            if vcol['field'] != 'id':
                selectedCols.append(vcol['field'])
        #print(selectedCols)
        #Let's load data from csv file into the data frame
        absFilePath="./datasets/" + selectedFile
        df=pd.read_csv(absFilePath, usecols=selectedCols) #nrows=200
        df = df.replace(np.nan, "Unknown")
        
        #Prepare the insert query
        spl=str()
        for i in range(len(inserColList)):
            if i == len(inserColList)-1:
                spl +='%s'
            else:    
                spl +='%s,'
        
        query="INSERT INTO " + tableName + "(" + str(",".join(inserColList)) + ") VALUES(" + str(spl) + ")"
        print(query)
        #convert data frame to dictionary and execute the insert query
        values=[]
        for index in range(0,len(df)):
            row=df.iloc[index].to_dict()
            values.append(list(row.values()))
            if (index+1)%50 == 0: 
                insertCount+=insertQuery(query, values)
                print(values)
                values = []
            elif index == len(df)-1:
                insertCount+=insertQuery(query, values)
            print(values)
    print("Total records inserted ", insertCount)
    return str(insertCount)+" of "+str(len(df))+" records inserted"
    
def droptable(selectedNode):
    query="DROP TABLE "+str(selectedNode['name'])
    dropstatus=dropQuery(query)
    return dropstatus

if __name__ == "__main__":
    db = connectToDb()
    cursor=db.cursor()
    loadToDb(db, cursor)
    #queryGreaterThanPriceDisplay(db,cursor)
    
    closeConnection(db, cursor)