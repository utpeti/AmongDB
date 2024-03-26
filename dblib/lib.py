import pymongo
from pymongo import MongoClient

cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')
mydb = cluster["matyi_test"]

TYPES = ['INT', 'FLOAT', 'BIT', 'DATE', 'DATETIME', 'VARCHAR']

### TABLE FUNCTIONS: ###

def create_table(name: str, content: str) -> str:
    if name in mydb.list_collection_names():
        msg = "A TABLE NAMED " + name + " ALREADY EXISTS IN THE DATABASE!"
        return msg
    mycol = mydb[name]
    # CREATING DICT FROM CONTENT
    tablestruct = {}
    for line in content.splitlines()[1:-1]:
        line = list(filter(None,line.strip(',').split(' ')))
        if line[1] in TYPES:
            tablestruct[line[0].strip()] = line[1]
    mycol.insert_one(tablestruct)
    msg = "TABLE " + name + " CREATED!"
    return msg
        
def drop_table(name: str) -> str:
    if name not in mydb.list_collection_names():
        msg = "A TABLE NAMED " + name + " DOES NOT EXIST IN THE DATABASE!"
        return msg
    mycol = mydb[name]
    mycol.drop()
    msg = "TABLE " + name + " DROPPED!"
    return msg
    
def list_tables():
    return mydb.list_collection_names()

### DATABASE FUNCTIONS: ###

def create_database(databaseName: str) -> str:
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        msg = "A DATABASE NAMED " + databaseName + " ALREADY EXISTS!"
    else:
        db = cluster[databaseName]
        initcollection = db[databaseName]
        initcollection.insert_one({'DATABASE CREATED': True})
        msg = "DATABASE " + databaseName + " CREATED!"
        #db.drop_collection(initcollection) emiatt letrejon de torlodik is a database
        #talan hasonlo modon meg lehetne oldani tablenel(?)
    return msg

def drop_database(databaseName: str) -> str:
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        cluster.drop_database(databaseName)
        msg = "DATABASE " + databaseName + " DROPPED!"
    else:
        msg = "DATABASE " + databaseName + " DOES NOT EXIST!"
    return msg
        
def list_databases():
    db_list = cluster.list_database_names()
    try:
        db_list.remove('admin')
        db_list.remove('local')
    except:
        pass
    return db_list

def select_curr_database(name: str) -> None:
    global mydb
    mydb = cluster[name]
    

### INDEX FUNCTIONS: ###

def create_index(indexName: str, tableName: str, column: str) -> str:
    column = column[0]
    if tableName in mydb.list_collection_names():
        collection = mydb[tableName]
        document = collection.find_one()
        if column.strip() not in document:
            msg = "NO COLUMN NAMED " + column + "!"
            return msg
        collection.create_index(column, name=str(indexName)) #maybe nem kell castelni, de igy megy so igy hagyom aztan ott rohad meg valoszinu
        msg = "INDEX " + indexName + " CREATED ON TABLE " + tableName + " FOR COLUMN: " + column
    else:
        msg = "NO TABLE NAMED " + tableName + "!"
    return msg
        