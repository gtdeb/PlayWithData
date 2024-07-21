from databases.mysql import *
from databases.mongo import *
from graph.seaborn_graph import *
from entity_model import Db

def getFrom_mysql():
    db_mysql = {'id': 'mysql', 'name': 'MySQL', 'type': 'sql', 'children': [getDatabaseTable_JSON()]}
    return db_mysql
    
def getfrom_mongo():
    db_mongo = {'id': 'mongo', 'name': 'Mongo', 'type': 'nosql', 'children': [getDatabaseCollection_JSON()]}
    return db_mongo
    
def maxRowId_table_mysql(req: Db):
    return maxRowId_table(req)

def maxRowId_collection_mongo(req: Db):
    return maxRowId_collection(req)

def readAndsendTableData_mysql(req: Db, fetchCount: int):
    #print("Fetch Count:- " + str(fetchCount))
    return getDataFromTable_JSON(req, fetchCount)

def readAndsendTableData_mongo(req: Db, fetchCount: int):
    return getDataFromCollection_JSON(req, fetchCount)
#
def insertTableData_mysql(req, addBuf):
    return insertTable_JSON(req, addBuf)
    
def insertTableData_mongo(req, addBuf):
    return insertCollection_JSON(req, addBuf)
    
def updateTableData_mysql(req, dirtyBuf):
    return updateTable_JSON(req, dirtyBuf)
    
def updateTableData_mongo(req, dirtyBuf):
    return updateCollection_JSON(req, dirtyBuf)
    
def deleteTableData_mysql(req, delBuf):
    return deleteTable_JSON(req, delBuf)
    
def deleteTableData_mongo(req, delBuf):
    return deleteCollection_JSON(req, delBuf)
    
def importdatamysql(selectedFile, tableName, vColumns, schemaData):
    return importdatamysql_table(selectedFile, tableName, vColumns, schemaData)

def importdatamongo(selectedFile, collectionName, vColumns):
    return importdatamongo_collection(selectedFile, collectionName, vColumns)

def droptablemysql(selectedNode):
    return droptable(selectedNode)

def dropcollectionmongo(selectedNode):
    return dropcollection(selectedNode)

def plot_graph(data):
    return plot_seaborn_graph(data)