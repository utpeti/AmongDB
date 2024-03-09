import pymongo
from pymongo import MongoClient

cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')

databasename = input()

db = cluster[databasename]
initcollection = db[databasename]
initcollection.insert_one({'init': 1})

print(cluster.list_database_names())