import pymongo
from pymongo import MongoClient

cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')
mydb = cluster["matyi_test"]

### TABLE FUNCTIONS: ###

def create_table(name: str) -> None:
    if name in mydb.list_collection_names():
        print("A TABLE NAMED " + name + " ALREADY EXISTS IN THE DATABASE!")
        return
    mycol = mydb[name]
    mycol.insert_one({'init': 1})
    print("TABLE " + name + " CREATED!")
        
def drop_table(name: str) -> None:
    if name not in mydb.list_collection_names():
        print("A TABLE NAMED " + name + " DOES NOT EXIST IN THE DATABASE!")
        return
    mycol = mydb[name]
    mycol.drop()
    print("TABLE " + name + " DROPPED!")
    
def list_tables() -> None:
    print(mydb.list_collection_names())

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

def drop_database(databaseName: str) -> None:
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        cluster.drop_database(databaseName)
        print("DATABASE " + databaseName + " DROPPED!")
    else:
        print("DATABASE " + databaseName + " DOES NOT EXIST!")

### INDEX FUNCTIONS: ###

