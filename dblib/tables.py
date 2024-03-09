import pymongo
from pymongo import MongoClient
import re

myclient = pymongo.MongoClient("mongodb+srv://matyi:1234@amongdb.xrci9ew.mongodb.net/")
mydb = myclient["matyi_test"]

create_table_regex = re.compile(r'Create Table (\w+)', re.IGNORECASE)
drop_table_regex = re.compile(r'Drop Table (\w+)', re.IGNORECASE)

def create_table(name: str) -> None:
    if name in mydb.list_collection_names():
        print("table with given name allready exists in the database")
        return
    mycol = mydb[name]
    mycol.insert_one({'init': 1})
    print("table created successfully")
        
def drop_table(name: str) -> None:
    if name not in mydb.list_collection_names():
        print("table with given name does not exist in the database")
        return
    mycol = mydb[name]
    mycol.drop()
    print("table deleted successfully")
    
def list_tables() -> None:
    print(mydb.list_collection_names())
    
    
def read_command():
    contents = []
    while True:
        line = input()
        contents.append(line)

        if line.endswith(';'):
            break
    command_str = " ".join(contents)
    return command_str

    # TODO: Sanitize input from spaces
    
while True:
    command = read_command()  
    if create_table_regex.match(command):
        valami = create_table_regex.search(command)
        create_table(valami.group(1))
    elif drop_table_regex.match(command):
        valami = drop_table_regex.search(command)
        drop_table(valami.group(1))
    else:
        print("Command not known")
        
        