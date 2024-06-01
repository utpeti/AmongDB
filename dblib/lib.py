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
    mycol_metadata = mydb[f'{name}.info']
    # CREATING DICT FROM CONTENT
    tablestruct = {}
    tablestruct['_id'] = 'ඞSTRUCTඞ'
    uniquestruct = {}
    uniquestruct['_id'] = 'ඞUNIQUE KEYSඞ'
    indexhandler = {}
    indexhandler['_id'] = 'ඞINDEXHANDLERඞ'
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
                        break
                except:
                    ...
        elif unique_key_regex.match(line):
            match = unique_key_regex.search(line)
            if match.group(1) in tablestruct.keys():
                uniquestruct[match.group(1)] = 0

        else:
            line = list(filter(None,line.strip(',').split(' ')))
            if line[1] in TYPES:
                tablestruct[line[0].strip()] = line[1]
            indexhandler[line[0].strip()] = None
    mycol_metadata.insert_one(tablestruct)
    mycol_metadata.insert_one(indexhandler)
    mycol.insert_one({'_id': 'ඞ', 'TABLE CREATED': True})
    mycol_metadata.insert_one(uniquestruct)
    msg = "TABLE " + name + " CREATED!"
    return msg
        
def drop_table(name: str) -> str:
    if name not in mydb.list_collection_names():
        msg = "A TABLE NAMED " + name + " DOES NOT EXIST IN THE DATABASE!"
        return msg
    mycol = mydb[name]
    mycol.drop()
    mydb[f'{name}.info'].drop()
    msg = "TABLE " + name + " DROPPED!"
    return msg
    
def list_tables2():
    return mydb.list_collection_names()

def list_tables():
    return  [item for item in mydb.list_collection_names() if not item.endswith('.info')]

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
def join_dic_strings(table_name1: str, table_name2: str, dic1: str, dic2: str, common: str):
    joined_dict = {}
    for key, val in string_to_dict(dic1).items():
        joined_dict[f'{table_name1}.{key}'] = val
    for key, val in string_to_dict(dic2).items():
        if key != common:
            joined_dict[f'{table_name2}.{key}'] = val
    return joined_dict

def build_join(source_name: str, other_name: str, source_table, other_table, col, index, pk1, pk2):
    inner_join_doc = []
    for doc in source_table.find({}):
        if doc['_id'] != 'ඞ':
            col_val = string_to_dict(doc['content'])[col]
            key1 = doc['_id']
            #searching for the value in the other table
            i = index.find(col_val)
            #KEY OF OTHER DOC
            if i > 0 and index[i-1] == '#':
                i2 = index.find('$', i)
                i3 = index.find('#', i2)
                if i3 > 0:
                    key2 = index[i2+1:i3]
                else:
                    key2 = index[i2+1:]
                for key in key2.split('ඞ'):
                    other_table_doc = other_table.find_one({'_id': key})
                    inner_join_doc.append(join_dic_strings(other_name, source_name, other_table_doc['content'] + f'#{pk2}:{key}', doc['content'] + f'#{pk1}:{key1}', col))
    return inner_join_doc

def first_inner_join(table1: str, table2: str, col1: str, col2: str):
    if table1 in mydb.list_collection_names() and table2 in mydb.list_collection_names():
        t1 = table1
        t2 = table2
        table1 = mydb[table1]
        table2 = mydb[table2]
        metadata1 = mydb[f'{t1}.info']
        metadata2 = mydb[f'{t2}.info']
        table_struct1 = metadata1.find_one({'_id': 'ඞSTRUCTඞ'})
        table_struct2 = metadata2.find_one({'_id': 'ඞSTRUCTඞ'})
        pk1 = table_struct1['KeyValue']
        pk2 = table_struct2['KeyValue']
        if col1 in table_struct1.keys() and col2 in table_struct2.keys():
            #indexes1 = table1.find_one({'_id': -1})
            #indexes2 = table2.find_one({'_id': -1})
            index_handler1 = metadata1.find_one({'_id': 'ඞINDEXHANDLERඞ'})
            index1 = index_handler1[col1]
            if index1 is not None:
                index = [x for x in index1.split('ඞ') if x]
                index = metadata1.find_one({'_id': index[0]})['VALUE']
                return build_join(t2, t1, table2, table1, col2, index, pk1, pk2)
            else:
                index_handler2 = metadata2.find_one({'_id': 'ඞINDEXHANDLERඞ'})
                index2 = index_handler2[col2]
                if index2 is not None:
                    index = [x for x in index2.split('ඞ') if x]
                    index = metadata1.find_one({'_id': index[0]})['VALUE']
                    return build_join(t2, t1, table2, table1, col2, index, pk2, pk1)
                else:
                    #create_index2('ඞ', t1, col1)
                    if col1 == table_struct1['KeyValue']:
                        l = []
                        for document in table1.find():
                            if document['_id'] != 'ඞ':
                                l.append(document['_id'])
                        l.sort(key=lambda x: int(x))
                        acc_string = ''
                        for key in l:
                            acc_string += f'#{key}${key}'
                    else:
                        l = []
                        for document in table1.find():
                            if document['_id'] != 'ඞ':
                                acc_dict = string_to_dict(document['content'])
                                l.append([document['_id'],acc_dict[col1]])
                        l.sort(key=lambda x: x[1])
                        acc_string = ''
                        prev_val = False
                        for key, val in l:
                            if val == prev_val:
                                acc_string += f'ඞ{key}'
                            else:
                                acc_string += f'#{val}${key}'
                                prev_val = val
                    return build_join(t2, t1, table2, table1, col2, index, pk1, pk2)
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

def nth_join_dic_and_string(doc, table_name2, table2, common):
    joined_dict = {}
    print(common)
    for key, val in doc.items():
        joined_dict[f'{key}'] = val
    for key, val in string_to_dict(table2).items():
        if key != common:
            joined_dict[f'{table_name2}.{key}'] = val
    return joined_dict

def nth_build_join(dict1, table2_name: str, table2, col, col2, index, pk2):
    inner_join_doc = []
    for doc in dict1:
        val = doc[col]
        i = index.find(val)
        if i > 0 and index[i-1] == '#':
            i2 = index.find('$', i)
            i3 = index.find('#', i2)
            if i3 > 0:
                key2 = index[i2+1:i3]
            else:
                key2 = index[i2+1:]
            for key in key2.split('ඞ'):
                acc = nth_join_dic_and_string(doc, table2_name, table2.find_one({'_id': key})['content'] + f'#{pk2}:{key}', col2)
                if acc:
                    inner_join_doc.append(acc)
    return inner_join_doc

def nth_inner_join(table1: dict, table2: str, col1: str, col2: str):
    if table2 not in mydb.list_collection_names():
        return f'{table2} DOESN\'T EXIST'
    t2 = table2
    table2 = mydb[table2]
    metadata2 = mydb[f'{t2}.info']
    table_struct2 = metadata2.find_one({'_id': 'ඞSTRUCTඞ'})
    pk2 = table_struct2['KeyValue']
    if col2 not in table_struct2:
        return f'{col2} DOESN\'T EXIST' 
    index_handler2 = metadata2.find_one({'_id': 'ඞINDEXHANDLERඞ'})
    index2 = index_handler2[col2]
    if index2 is not None:
        index = [x for x in index2.split('ඞ') if x]
        index = metadata2.find_one({'_id': index[0]})['VALUE']
        return nth_build_join(table1, t2, table2, col1, col2, index, pk2)
    else:
        l = []
        for document in table2.find():
            if document['_id'] != 'ඞ':
                acc_dict = string_to_dict(document['content'])
                l.append([document['_id'],acc_dict[col2]])
            l.sort(key=lambda x: x[1])
            acc_string = ''
            prev_val = False
            for key, val in l:
                if val == prev_val:
                    acc_string += f'ඞ{key}'
                else:
                    acc_string += f'#{val}${key}'
                    prev_val = val
        return nth_build_join(table1, t2, table2, col1, col2, acc_string, pk2)

def rest_preproccess(rest: str):
    rest = rest.strip()
    rest = rest.split('JOIN')
    rest = [x.strip() for x in rest if x]
    ans = []
    for s in rest:
        table_name = s[0:s.find(' ')]
        col1 = s[s.find('ON')+3:s.find(' ', s.find('ON')+3)]
        col2 = s[s.rfind(' ') + 1:]
        ans.append([table_name,col1,col2])
    return ans

def inner_join_handler(table1: str, table2: str, col1: str, col2: str, rest: str):
    ans = first_inner_join(table1, table2, col1, col2)
    if len(rest.strip()) > 0:
        tables = rest_preproccess(rest)
        for content in tables:
            ans = nth_inner_join(ans, content[0], content[1], content[2])
    return ans


### INDEX FUNCTIONS: ###

def update_index_handler(indexName: str, tableName: str, column: str):
    metadata = mydb[f'{tableName}.info']
    handler = metadata.find_one({'_id': 'ඞINDEXHANDLERඞ'})
    indexes = handler[column]
    if not indexes:
        indexes = ''
    indexes += f'ඞ{indexName}'
    metadata.update_one(handler, {'$set': {column: indexes} })
    
def magic(indexName: str, collection, column, metadata):
    l = []
    for document in collection.find():
        if document['_id'] != 'ඞ':
            acc_dict = string_to_dict(document['content'])
            l.append([document['_id'],acc_dict[column]])
    l.sort(key=lambda x: x[1])
    acc_string = ''
    prev_val = False
    for key, val in l:
        if val == prev_val:
            acc_string += f'ඞ{key}'
        else:
            acc_string += f'#{val}${key}'
            prev_val = val
    metadata.insert_one({'_id': indexName, 'VALUE': acc_string})
    #collection.update_one(indexes, {'$set': {indexName: acc_string} })

def magicPK(indexName: str, collection, metadata):
    l = []
    for document in collection.find():
        if document['_id'] != 'ඞ':
            l.append(document['_id'])
    l.sort(key=lambda x: int(x))
    acc_string = ''
    for key in l:
        acc_string += f'#{key}${key}'
    metadata.insert_one({'_id': indexName, 'VALUE': acc_string})

def create_index2(indexName: str, tableName: str, column: str) -> str:
    column = column[0]
    if tableName in mydb.list_collection_names():
        collection = mydb[tableName]
        metadata = mydb[f'{tableName}.info']
        if metadata.find_one({'_id': indexName}):
            return 'INDEX ALLREADY EXISTS'
        document = metadata.find_one({'_id': 'ඞSTRUCTඞ'})
        if column.strip() not in document and column.strip() != 'KeyValue':
            msg = "NO COLUMN NAMED " + column + "!"
            return msg
        if column.strip() == document['KeyValue']:
            magicPK(indexName, collection, metadata)
        else:
            magic(indexName, collection, column, metadata)
        update_index_handler(indexName, tableName, column)
        #collection.create_index(column, name=str(indexName)) #maybe nem kell castelni, de igy megy so igy hagyom aztan ott rohad meg valoszinu
        msg = "INDEX " + indexName + " CREATED ON TABLE " + tableName + " FOR COLUMN: " + column
    else:
        msg = "NO TABLE NAMED " + tableName + "!"
    return msg

def index_to_dict(index):
    return {el[0]: el[1].split('ඞ') for el in [x.split('$') for x in index['VALUE'][1:].split('#')]}

def update_index(value, metadata, index, id, method):
    index = metadata.find_one({'_id': index})
    ind = index_to_dict(index)
    if method == 'add':
        if value in ind:
            ind[value].append(id)
        else:
            ind[value] = [id]
    elif method == 'remove':
        ind[value].remove(id)
        if not ind[value] :
            del ind[value]
    ind = '#' + '#'.join(f'{val}${"ඞ".join(ids)}' for val, ids in sorted(ind.items()))
    metadata.update_one(index, {'$set': {'VALUE': ind} })

def update_all_indexes(values, metadata, id, method):
    index_handler = metadata.find_one({'_id': 'ඞINDEXHANDLERඞ'})
    for column in values.keys():
        indexes = index_handler[column]
        if not indexes:
            return
        for index in [x for x in indexes.split('ඞ') if x]:
            update_index(values[column], metadata, index, id, method)
        
def get_index_on_col(col):
    ...

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
        return "COLUMNS AND VALUES SHOULD HAVE THE SAME AMOUNT"
    if tablename in mydb.list_collection_names():
        collection = mydb[tablename]
        metadata = mydb[f'{tablename}.info']
        struct = metadata.find_one({'_id': 'ඞSTRUCTඞ'})
        struct.pop('_id')
        pk = struct['KeyValue']
        keyVal = 0
        i = 0
        msg = ''
        for col in dest:
            if col in struct:
                if typecheck(content[i], struct[col]):
                    msg += f' {content[i]} = {struct[col]} ' 
                if col == struct['KeyValue']:
                    keyVal = content[i]
                    pk_val = keyVal
                    struct.pop(col)
                else:
                    struct[col] = content[i]
            else:
                return col + ' NOT IN ' + tablename + msg
            i += 1
        struct.pop('KeyValue')
        content_string = dict_to_string(struct)
        struct[pk] = pk_val
        update_all_indexes(struct, metadata, keyVal, 'add')
        print(struct)
        collection.insert_one({'_id' : keyVal,'content': content_string})
    return msg


def delete_doc_exact(table_name, col, val):
    if table_name not in mydb.list_collection_names():
        return 'TABLE DOES NOT EXIST'
    collection = mydb[table_name]
    struct = mydb[f'{table_name}.info'].find_one({'_id': 'ඞSTRUCTඞ'})
    struct.pop('_id')
    if not struct.get(col,False):
        return "COL DOESNT EXIST"
    metadata = mydb[f'{table_name}.info']
    index_handler = metadata.find_one({ '_id': 'ඞINDEXHANDLERඞ'})
    indexes = index_handler[col]
    if indexes is not None:
        index = [x for x in indexes.split('ඞ') if x]
        index = metadata.find_one({'_id': index[0]})
        ind = index_to_dict(index)
        ids = ind[val]
        for id in ids:
            document = collection.find_one({'_id': id})
            document_dict = string_to_dict(document['content'])
            document_dict[struct['KeyValue']] = id
            collection.delete_one(document)
            update_all_indexes(document_dict, metadata, id, 'remove')

    else:
        for document in collection.find():
            acc = string_to_dict(document['content']) #TODO: indexes and primary key implement
            if acc[col] == val:
                document_dict = string_to_dict(acc)
                document_dict[struct['KeyValue']] = id
                collection.delete_one(document)
                update_all_indexes(document_dict, metadata, id, 'remove') #TUDOM MEGTUDTAM VOLNA DROP MANYVEL IS OLDANI DE IGY FOGTAM NEKI, HAJNALI 3 VAN ES MAR ALUDNI AKAROK HA NEKED EZZEL VAN VALAMI BAJOD AKKOR IRD MEG MAGADNAK VAGY VARDD MEG MIG FELKELEK ES ATIROM EN, AMUGYIS AZ EN BRANCHEMBEN VAN EZ, SO MEG NEM COMPLETE UGY MINT AHOGY EN LETTEM ETTOL COMPLETE NEBUN
            
    return "na valami"


#passek helyett majd valami error uzenetet berakok

def typecheck(val, typ:str):
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

### SELECT ###

def select_output_formatting(d: dict) -> str:
    for key, value in d.items():
        d[key] = ', '.join(value)
    return d

def select_all(table_name: str):
    collection = mydb[table_name]
    struct = collection.find_one({'_id': 0})
    ret_dict = {}
    for key in struct.keys():
        if key not in ['_id', 'KeyValue']:
            ret_dict[key] = []
    if struct:
        struct.pop('_id')
        for document in collection.find():
            if document['_id'] not in [0,-1,-3]:
                acc_dict = string_to_dict(document['content'])
                for key, value in acc_dict.items():
                    ret_dict[key].append(value)
                    
        return select_output_formatting(ret_dict)
    return "TABLE EMPTY"

def select_col(col_names, table_name: str):
    collection = mydb[table_name]
    struct = collection.find_one({'_id': 0})
    ret_dict = {}
    for key in struct.keys():
        if key not in ['_id', 'KeyValue'] and key in col_names:
            ret_dict[key] = []
    if struct:
        struct.pop('_id')
        for document in collection.find():
            if document['_id'] not in [0,-1,-3]:
                acc_dict = string_to_dict(document['content'])
                for key, value in acc_dict.items():
                    if key in col_names:
                        ret_dict[key].append(value)
        return select_output_formatting(ret_dict)
    return "TABLE EMPTY"

def select_all_where(table_name: str, conditions: str):
    collection = mydb[table_name]
    struct = collection.find_one({'_id': 0})
    ret_dict = {}
    for key in struct.keys():
        if key not in ['_id', 'KeyValue']:
            ret_dict[key] = []
    if struct:
        struct.pop('_id')
        for document in collection.find():
            if document['_id'] not in [0, -1, -3]:
                acc_dict = string_to_dict(document['content'])
                if evaluate_conditions(acc_dict, conditions):
                    for key in acc_dict.keys():
                        if key in ret_dict:
                            ret_dict[key].append(acc_dict[key])
        
        return select_output_formatting(ret_dict)
    return "TABLE EMPTY"

def select_where(col_names, table_name: str, conditions: str):
    collection = mydb[table_name]
    struct = collection.find_one({'_id': 0})
    renames = []
    ret_dict = {}
    
    for col_name in col_names:
        if 'AS' in col_name:
            parts = col_name.split('AS')
            if len(parts) == 2:
                original_col, alias = parts[0].strip(), parts[1].strip()
                renames.append((original_col, alias))
            else:
                raise ValueError(f"Invalid column alias format in: {col_name}")
        else:
            renames.append((col_name.strip(), col_name.strip()))
    
    for original_col, alias in renames:
        ret_dict[alias] = []
    
    if struct:
        struct.pop('_id', None)
        for document in collection.find():
            if document['_id'] not in [0, -1, -3]:
                acc_dict = string_to_dict(document['content'])
                
                if evaluate_conditions(acc_dict, conditions):
                    for original_col, alias in renames:
                        if original_col in acc_dict:
                            ret_dict[alias].append(acc_dict[original_col])
    
        return select_output_formatting(ret_dict)
    return "TABLE EMPTY"


def evaluate_conditions(data, conditions):
    condition_pattern = re.compile(r"([A-Za-z0-9_]+)\s*(=|>=|<=|>|<)\s*('.*?'|[A-Za-z0-9_]+)\s*(AND|OR)?", re.IGNORECASE)
    matches = condition_pattern.findall(conditions)
    
    if not matches:
        return False
    
    result = None
    current_operator = None

    for match in matches:
        column, operator, value, logical_op = match
        column = column.strip()
        value = value.strip().strip("'")
        
        if column not in data:
            return False
        
        condition_met = evaluate_condition(data[column], operator, value)

        if result is None:
            result = condition_met
        elif current_operator == "AND":
            result = result and condition_met
        elif current_operator == "OR":
            result = result or condition_met
        
        current_operator = logical_op.strip().upper() if logical_op else None
    
    return result

def evaluate_condition(data_value, operator, condition_value):
    try:
        data_value = int(data_value)
        condition_value = int(condition_value)
    except ValueError:
        pass

    if operator == '=':
        return data_value == condition_value
    elif operator == '>':
        return data_value > condition_value
    elif operator == '<':
        return data_value < condition_value
    elif operator == '>=':
        return data_value >= condition_value
    elif operator == '<=':
        return data_value <= condition_value
    return False