import pymongo
from pymongo import MongoClient

cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')

def create_database(databaseName):
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        print("A DATABASE NAMED " + databaseName + " ALREADY EXISTS!")
    else:
        db = cluster[databaseName]
        initcollection = db[databasename]
        initcollection.insert_one({'DATABASE CREATED': True})
        print("DATABASE " + databaseName + " CREATED!")
        #db.drop_collection(initcollection) emiatt letrejon de torlodik is a database

def drop_database(databaseName) -> None:
    dbnames = cluster.list_database_names()
    if databaseName in dbnames:
        cluster.drop_database(databaseName)
        print("DATABASE " + databaseName + " DROPPED!")
    else:
        print("DATABASE " + databaseName + " DOES NOT EXIST!")




databasename = input()
drop_database(databasename)
