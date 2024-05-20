from pymongo import MongoClient
import re
import random
from datetime import datetime

cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')
mydb = cluster["matyi_test"]

TYPES = ['INT', 'FLOAT', 'BIT', 'DATE', 'DATETIME', 'VARCHAR']
primary_key_regex = re.compile(r'\s+PRIMARY\s+KEY\s+\((\w+)\)', re.IGNORECASE)
foreign_key_regex = re.compile(r'\s+FOREIGN\s+KEY\s+\((\w+)\)', re.IGNORECASE)
unique_key_regex = re.compile(r'\s+UNIQUE\s+KEY\s+\((\w+)\)', re.IGNORECASE)

### TABLE FUNCTIONS: ###

#TODO: check if keyvalue exists

def create_table(name: str, content: str) -> str:
    if name in mydb.list_collection_names():
        msg = "A TABLE NAMED " + name + " ALREADY EXISTS IN THE DATABASE!"
        return msg
    mycol = mydb[name]
    # CREATING DICT FROM CONTENT
    tablestruct = {}
    tablestruct['_id'] = 0
    uniquestruct = {}
    uniquestruct['_id'] = -3
    for line in content.splitlines()[1:-1]:
        line.strip(',')
        if primary_key_regex.match(line):
            match = primary_key_regex.search(line)
            tablestruct['KeyValue'] = match.group(1)
            uniquestruct[match.group(1)] = 0
        elif foreign_key_regex.match(line):
            match = foreign_key_regex.search(line)
            got_one = False
            #SEARCH FOR TABLE WHERE var IS PRIMARY KEY
            for table_name in mydb.list_collection_names() :
                try:
                    table = mydb[table_name]
                    table = table.find_one({'_id' : 0})
                    if table['KeyValue'] == match.group(1):
                        got_one = True
                        print('megkapta')
                        break
                except:
                    ...
        elif unique_key_regex.match(line):
            match = unique_key_regex.search(line)
            if match.group(1) in tablestruct.keys():
                uniquestruct[match.group(1)] = 0

        line = list(filter(None,line.strip(',').split(' ')))
        if line[1] in TYPES:
            tablestruct[line[0].strip()] = line[1]
    mycol.insert_one(tablestruct)
    indexstruct = {}
    indexstruct['_id'] = -1
    mycol.insert_one(indexstruct)
    mycol.insert_one(uniquestruct)
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

### JOIN ###


def first_inner_join(table1: str, table2: str, col1: str, col2: str):
    index = ''
    def index_srch(indexes: dict, col: str):
        global index
        for val in indexes.values():
            print([val,col])
            if str(val).startswith(col):
                index = val
                return True
        return False

    if table1 in mydb.list_collection_names() and table2 in mydb.list_collection_names():
        table1 = mydb[table1]
        table2 = mydb[table2]
        table_struct1 = table1.find_one({'_id': 0})
        table_struct2 = table2.find_one({'_id': 0})
        if col1 in table_struct1.keys() and col2 in table_struct2.keys():
            indexes1 = table1.find_one({'_id': -1})
            indexes2 = table2.find_one({'_id': -1})
            if index_srch(indexes1, col1):
                print(index)
                for doc in table2.find({}):
                    if doc['_id'] not in [0,-1,-3]:
                        col_val = string_to_dict(doc['content'])[col2]
                        print(type(col_val))
                        i = index.find(col_val)
                        if index[i-1] == '#':
                            i2 = index.find('$', i)
            elif index_srch(indexes2, col2):
                ...
            else:
                ...
        else:
            if col1 not in table_struct1.keys():
                return f'{col1} DOESN\'T EXIST'
            else:
                return f'{col2} DOESN\'T EXIST'
    else:
        if table1 not in mydb.list_collection_names():
            return f'{table1} DOESN\'T EXIST'
        else:
            return f'{table2} DOESN\'T EXIST'
    return 'ja'

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

def somethingisfucked2(indexName: str, collection, column):
    indexes = collection.find_one({'_id': -1})
    if indexName in indexes:
        return 'INDEX ALLREADY EXISTS'
    l = []
    for document in collection.find():
        if document['_id'] not in [0,-1,-3]:
            acc_dict = string_to_dict(document['content'])
            l.append([document['KeyValue'],acc_dict[column]])
    l.sort(key=lambda x: x[1])
    acc_string = '#'.join(x[0] for x in l)
    collection.update_one(indexes, {'$set': {indexName: acc_string} })
    
def somethingisfucked(indexName: str, collection, column):
    indexes = collection.find_one({'_id': -1})
    if indexName in indexes:
        return 'INDEX ALLREADY EXISTS'
    l = []
    for document in collection.find():
        if document['_id'] not in [0,-1,-3]:
            acc_dict = string_to_dict(document['content'])
            l.append([document['KeyValue'],acc_dict[column]])
    l.sort(key=lambda x: x[1])
    #acc_string = '#'.join(x[0] for x in l)
    acc_string = f'{column}'
    prev_val = False
    for key, val in l:
        if val == prev_val:
            acc_string += f'ඞ{key}'
        else:
            acc_string += f'#{val}${key}'
        prev_val = val
    collection.update_one(indexes, {'$set': {indexName: acc_string} })


def create_index2(indexName: str, tableName: str, column: str) -> str:
    column = column[0]
    if tableName in mydb.list_collection_names():
        collection = mydb[tableName]
        document = collection.find_one({'_id': 0})
        if column.strip() not in document:
            msg = "NO COLUMN NAMED " + column + "!"
            return msg
        somethingisfucked(indexName, collection, column)
        #collection.create_index(column, name=str(indexName)) #maybe nem kell castelni, de igy megy so igy hagyom aztan ott rohad meg valoszinu
        msg = "INDEX " + indexName + " CREATED ON TABLE " + tableName + " FOR COLUMN: " + column
    else:
        msg = "NO TABLE NAMED " + tableName + "!"
    return msg
        
        
### DOC FUNC ###
                
def insertDoc(tablename: str, dest: str, content: str) -> str:
    msg = ''
    if len(dest) != len(content):
        return "COLUMNS AND VALUES SHOULD HAVE THE SAME AMOUNT"
    if tablename in mydb.list_collection_names():
        collection = mydb[tablename]
        struct = collection.find_one({'_id': 0})
        struct.pop('_id')
        i = 0
        for col in dest:
            if col in struct:
                struct[col] = content[i]
            else:
                return col + ' NOT IN ' + tablename
            i += 1
        content_string = dict_to_string(struct)
        collection.insert_one({'content': content_string})
    return "worked"

### DOC FUNC ###
def dict_to_string(d) -> str:
    return "#".join(f"{k}:{v}" for k, v in d.items())

def string_to_dict(string) -> dict:
    return dict(item.split(':') for item in string.split('#'))

#TYPES = ['INT', 'FLOAT', 'BIT', 'DATE', 'DATETIME', 'VARCHAR']

def dict_set_default(d) -> dict:
    cpy = {}
    for k, v in d.items():
        acc = v
        match v:
            case 'INT':
                acc = 'NULL'
            case 'FLOAT':
                acc = 'NULL'
            case 'BIT':
                acc = 'NULL'
            case 'DATE':
                acc = 'NULL'
            case 'DATETIME':
                acc = 'NULL'
            case 'VARCHAR':
                acc = 'NULL'

def insertDoc(tablename: str, dest: str, content: str) -> str:
    msg = ''
    if len(dest) != len(content):
        print('hossz hiba')
        return "COLUMNS AND VALUES SHOULD HAVE THE SAME AMOUNT"
    if tablename in mydb.list_collection_names():
        collection = mydb[tablename]
        struct = collection.find_one({'_id': 0})
        struct.pop('_id')
        keyVal = 0
        i = 0
        msg = ''
        for col in dest:
            if col in struct:
                if typecheck(content[i], struct[col]):
                    msg += f' {content[i]} = {struct[col]} ' 
                if col == struct['KeyValue']:
                    keyVal = content[i]
                    struct.pop(col)
                else:
                    struct[col] = content[i]
            else:
                return col + ' NOT IN ' + tablename + msg
            i += 1
        struct.pop('KeyValue')
        content_string = dict_to_string(struct)
        collection.insert_one({'KeyValue' : keyVal,'content': content_string})
    print('idk')
    return msg


def delete_doc_exact(table_name, col, val):
    collection = mydb[table_name]
    struct = collection.find_one({'_id': 0})
    if not struct.get(col,False):
        return "fasz"
    for document in collection.find():
        if document['_id'] != 0:
            acc = string_to_dict(document['content'])
            if acc[col] == val:
                collection.delete_one(document) #TUDOM MEGTUDTAM VOLNA DROP MANYVEL IS OLDANI DE IGY FOGTAM NEKI, HAJNALI 3 VAN ES MAR ALUDNI AKAROK HA NEKED EZZEL VAN VALAMI BAJOD AKKOR IRD MEG MAGADNAK VAGY VARDD MEG MIG FELKELEK ES ATIROM EN, AMUGYIS AZ EN BRANCHEMBEN VAN EZ, SO MEG NEM COMPLETE UGY MINT AHOGY EN LETTEM ETTOL COMPLETE NEBUN
    return "na valami"


def trust():
    for i in range(1024):
        dest = ['a','b','c','d','e','f','g']
        my_list = [random.randint(10, 10000) for _ in range(7)]
        my_list[0] = i
        my_list = [str(x) for x in my_list]
        insertDoc('teszt', dest, my_list)
        print(i)

#passek helyett majd valami error uzenetet berakok

def typecheck(val, typ:str):
    print([val,typ])
    match typ:
        case 'INT':
            try:
                int(val)
                return True
            except ValueError:
                pass
        case 'FLOAT':
            try:
                float(val)
                return True
            except ValueError:
                pass
        case 'BIT':
            try:
                bool(val)
                return True
            except ValueError:
                pass
        case 'DATE':
            try:
                datetime.strptime(val, '%Y/%m/%d')
                return True
            except ValueError:
                pass
        case 'DATETIME':
            try:
                datetime.strptime(val, '%Y/%m/%d %H:%M:%S')
                return True
            except ValueError:
                pass
        case 'VARCHAR':
            return True
    return False


'''
delete from test where a = 2;
'''
"a"