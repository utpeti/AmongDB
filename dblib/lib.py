import pymongo
from pymongo import MongoClient

cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')
mydb = cluster["matyi_test"]

TYPES = ['INT', 'FLOAT', 'BIT', 'DATE', 'DATETIME', 'VARCHAR']

### TABLE FUNCTIONS: ###

def create_table(name: str, content: str) -> None:
    if name in mydb.list_collection_names():
        print("A TABLE NAMED " + name + " ALREADY EXISTS IN THE DATABASE!")
        return
    mycol = mydb[name]
    # CREATING DICT FROM CONTENT
    tablestruct = {}
    for line in content.splitlines()[1:-1]:
        line = list(filter(None,line.strip(',').split(' ')))
        if line[1] in TYPES:
            tablestruct[line[0]] = line[1]
    mycol.insert_one(tablestruct)
    print("TABLE " + name + " CREATED!")
        
def drop_table(name: str) -> None:
    if name not in mydb.list_collection_names():
        print("A TABLE NAMED " + name + " DOES NOT EXIST IN THE DATABASE!")
        return
    mycol = mydb[name]
    mycol.drop()
    print("TABLE " + name + " DROPPED!")
    
def list_tables():
    return mydb.list_collection_names()

### DATABASE FUNCTIONS: ###

def create_database(databaseName: str) -> None:
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        print("A DATABASE NAMED " + databaseName + " ALREADY EXISTS!")
    else:
        db = cluster[databaseName]
        initcollection = db[databaseName]
        initcollection.insert_one({'DATABASE CREATED': True})
        print("DATABASE " + databaseName + " CREATED!")
        #db.drop_collection(initcollection) emiatt letrejon de torlodik is a database
        #talan hasonlo modon meg lehetne oldani tablenel(?)

def drop_database(databaseName: str) -> None:
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        cluster.drop_database(databaseName)
        print("DATABASE " + databaseName + " DROPPED!")
    else:
        print("DATABASE " + databaseName + " DOES NOT EXIST!")
        
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

def create_index(indexName: str, tableName: str, columns: str) -> None:
    if tableName in mydb.list_collection_names():
        collection = mydb[tableName]
        document = collection.find_one()
        
        for column in columns:
            if column.strip() not in document:
                print("NO COLUMN NAMED " + column + "!")
                return
        
        for column in columns:
            collection.create_index(column, name=str(indexName)) #maybe nem kell castelni, de igy megy so igy hagyom aztan ott rohad meg valoszinu
            
        print(f"INDEX '{indexName}' CREATED ON TABLE '{tableName}' FOR COLUMNS: {columns}")
    else:
        print("NO TABLE NAMED " + tableName + "!")
        