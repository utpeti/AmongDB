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
inner_join_regex_for_select = re.compile(r'FROM\s([A-Za-z0-9_]+)\s+JOIN\s+([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)\s+=\s+([A-Za-z0-9_]+)([^)]*)', re.IGNORECASE)
condition_pattern = re.compile(r"([A-Za-z0-9_.]+)\s*(=|>=|<=|>|<)\s*('.*?'|[A-Za-z0-9_.]+)\s*(AND|OR)?", re.IGNORECASE)

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
    innertablestructwhichwillberestored = []
    uniquestruct = {}
    uniquestruct['_id'] = 'ඞUNIQUE KEYSඞ'
    indexhandler = {}
    indexhandler['_id'] = 'ඞINDEXHANDLERඞ'
    pk = ''
    for line in content.splitlines()[1:-1]:
        line.strip(',')
        if primary_key_regex.match(line):
            match = primary_key_regex.search(line)
            innertablestructwhichwillberestored.append(['KeyValue', match.group(1)])
            pk = match.group(1)
            #tablestruct['KeyValue'] = match.group(1)
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
                #tablestruct[line[0].strip()] = line[1]
                innertablestructwhichwillberestored.append([line[0].strip(), line[1]])
            indexhandler[line[0].strip()] = None
    tablestruct['cols'] = innertablestructwhichwillberestored
    mycol_metadata.insert_one(tablestruct)
    mycol_metadata.insert_one(indexhandler)
    mycol.insert_one({'_id': 'ඞ', 'TABLE CREATED': True})
    mycol_metadata.insert_one(uniquestruct)
    msg = "TABLE " + name + " CREATED!"
    create_index2('primarykeyindex', name, pk)
    return msg

def backtonormalstruct(structorder):
    struct = structorder.copy()
    acc = struct['cols']
    del struct['cols']
    for coll, typ in acc:
        struct[coll] = typ
    return struct
        
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
            joined_dict[f'{table_name2}.{key}'] = val
    return joined_dict

def build_join(source_name: str, other_name: str, source_table, other_table, col, index, pk1, pk2, structorder_source, structorder_other, ids_source, ids_other):
    inner_join_doc = []
    #WE ONLY SELECT THE NECESSARY IDS (WE FILTER THEM OUT)
    for doc in source_table.find({'_id': {'$in': ids_source}}):
        if doc['_id'] != 'ඞ':
            key1 = doc['_id']
            col_val = string_to_dict(backtonormalstring(doc['content'], structorder_source)+ f'#{pk1}:{key1}')[col]
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
                    if key in ids_other:
                        other_table_doc = other_table.find_one({'_id': key})
                        if other_table_doc is None:
                            #continue
                            ...
                        inner_join_doc.append(join_dic_strings(other_name, source_name, backtonormalstring(other_table_doc['content'], structorder_other) + f'#{pk2}:{key}', backtonormalstring(doc['content'], structorder_source) + f'#{pk1}:{key1}', col))
    return inner_join_doc
#TODO: add indexfilter
def first_inner_join(table1: str, table2: str, col1: str, col2: str, conditions):
    if table1 in mydb.list_collection_names() and table2 in mydb.list_collection_names():
        t1 = table1
        t2 = table2
        table1 = mydb[table1]
        table2 = mydb[table2]
        metadata1 = mydb[f'{t1}.info']
        metadata2 = mydb[f'{t2}.info']
        structorder1 = metadata1.find_one({'_id': 'ඞSTRUCTඞ'})
        table_struct1 = backtonormalstruct(structorder1)
        structorder2 = metadata2.find_one({'_id': 'ඞSTRUCTඞ'})
        table_struct2 = backtonormalstruct(structorder2)
        pk1 = table_struct1['KeyValue']
        pk2 = table_struct2['KeyValue']
        index_handler1 = metadata1.find_one({'_id': 'ඞINDEXHANDLERඞ'})
        index1 = index_handler1[col1]
        index_handler2 = metadata2.find_one({'_id': 'ඞINDEXHANDLERඞ'})
        index2 = index_handler2[col2]
        ids1 = indexfilter(metadata1, index_handler1, conditions, table1, t1)
        #print(ids1)
        ids2 = indexfilter(metadata2, index_handler2, conditions, table2, t2)
        #print(ids2)
        if col1 in table_struct1.keys() and col2 in table_struct2.keys():
            #indexes1 = table1.find_one({'_id': -1})
            #indexes2 = table2.find_one({'_id': -1})
            
            if index1 is not None:
                index = [x for x in index1.split('ඞ') if x]
                index = metadata1.find_one({'_id': index[0]})['VALUE']
                return build_join(t2, t1, table2, table1, col2, index, pk2, pk1, structorder2, structorder1, ids2, ids1)
            else:
                if index2 is not None:
                    index = [x for x in index2.split('ඞ') if x]
                    index = metadata2.find_one({'_id': index[0]})['VALUE']
                    return build_join(t1, t2, table1, table2, col1, index, pk1, pk2, structorder1, structorder2, ids1, ids2)
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
                                acc_dict = string_to_dict(backtonormalstring(document['content'], structorder1))
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
                    return build_join(t2, t1, table2, table1, col2, acc_string, pk2, pk1, structorder2, structorder1, ids2, ids1)
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
    structorder2 = metadata2.find_one({'_id': 'ඞSTRUCTඞ'})
    table_struct2 = backtonormalstruct(structorder2)
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
                acc_dict = string_to_dict(backtonormalstring(document['content'], structorder2))
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

def inner_join_handler(table1: str, table2: str, col1: str, col2: str, rest: str, conditions):
    ans = first_inner_join(table1, table2, col1, col2, conditions)
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
    
def magic(indexName: str, collection, column, metadata, structorder):
    l = []
    for document in collection.find():
        if document['_id'] != 'ඞ':
            acc_dict = string_to_dict(backtonormalstring(document['content'], structorder))
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
        structorder = metadata.find_one({'_id': 'ඞSTRUCTඞ'})
        document = backtonormalstruct(structorder)
        if column.strip() not in document and column.strip() != 'KeyValue':
            msg = "NO COLUMN NAMED " + column + "!"
            return msg
        if column.strip() == document['KeyValue']:
            magicPK(indexName, collection, metadata)
        else:
            magic(indexName, collection, column, metadata, structorder)
        update_index_handler(indexName, tableName, column)
        #collection.create_index(column, name=str(indexName)) #maybe nem kell castelni, de igy megy so igy hagyom aztan ott rohad meg valoszinu
        msg = "INDEX " + indexName + " CREATED ON TABLE " + tableName + " FOR COLUMN: " + column
    else:
        msg = "NO TABLE NAMED " + tableName + "!"
    return msg

def index_to_dict(index):
    return {el[0]: el[1].split('ඞ') for el in [x.split('$') for x in index['VALUE'][1:].split('#')]}

def update_index(value, metadata, index, id, method):
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
        for index in metadata.find({'_id': {'$in': [x for x in indexes.split('ඞ') if x]}}):
            update_index(values[column], metadata, index, id, method)
        
def get_all_indexes_on_col(col):
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

def tonewstring(d, structorder, pk):
    l = []
    for k, t in structorder['cols']:
        if k != 'KeyValue' and k != pk:
            l.append(d[k])
    return '#'.join(l)

def backtonormalstring(newstring, structorder):
    l2 = newstring.split('#')
    for k, t in structorder['cols']:
        if k == 'KeyValue':
            pk = t
    i = 0
    for k, t in structorder['cols']:
        if k != 'KeyValue' and k != pk:
            l2[i] = f'{k}:{l2[i]}'
            i += 1
    return '#'.join(l2)

def insertDoc(tablename: str, dest: str, content: str) -> str:
    msg = ''
    if len(dest) != len(content):
        return "COLUMNS AND VALUES SHOULD HAVE THE SAME AMOUNT"
    if tablename in mydb.list_collection_names():
        collection = mydb[tablename]
        metadata = mydb[f'{tablename}.info']
        structorder = metadata.find_one({'_id': 'ඞSTRUCTඞ'})
        struct = backtonormalstruct(structorder)
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
        #content_string = dict_to_string(struct)
        content_string = tonewstring(struct, structorder, pk)
        struct[pk] = pk_val
        update_all_indexes(struct, metadata, keyVal, 'add')
        collection.insert_one({'_id' : keyVal,'content': content_string})
    return msg

def get_index_names_from_handler(handler: dict):
    del handler['_id']
    return [y for y in ''.join([x for x in handler.values() if x is not None]).split('@') if y]

def delete_doc_exact(table_name, col, val):
    if table_name not in mydb.list_collection_names():
        return 'TABLE DOES NOT EXIST'
    collection = mydb[table_name]
    structorder = mydb[f'{table_name}.info'].find_one({'_id': 'ඞSTRUCTඞ'})
    struct = backtonormalstruct(structorder)
    struct.pop('_id')
    if not struct.get(col,False):
        return "COL DOESNT EXIST"
    metadata = mydb[f'{table_name}.info']
    index_handler = metadata.find_one({ '_id': 'ඞINDEXHANDLERඞ'})
    indexes = index_handler[col]
    if indexes is not None:
        index_list = [x for x in indexes.split('ඞ') if x]
        index = metadata.find_one({'_id': index_list[0]})
        ind = index_to_dict(index)
        ids = ind[val]
        for id in ids:
            document = collection.find_one({'_id': id})
            collection.delete_one(document)

    else:
        for document in collection.find():
            if document['_id'] != 'ඞ':

                acc = string_to_dict(backtonormalstring(document['content'], structorder)) #TODO: indexes and primary key implement
                if acc[col] == val:
                    collection.delete_one(document)

    index_handler_update = index_handler.copy()
    for key in index_handler_update.keys():
        if key != '_id':
            index_handler_update[key] = None
    wait = metadata.update_one({'_id': index_handler_update['_id']}, {'$set': index_handler_update})


    del index_handler['_id']
    ind_list = {k: [y for y in v.split('ඞ') if y] for k, v in index_handler.items() if v is not None and any(v.split('ඞ'))}
    all_indexes = [item for sublist in ind_list.values() for item in sublist]
    metadata.delete_many({'_id': {'$in': all_indexes}})
    #wait for the server to delete
    while metadata.count_documents({}) > 3:
        ...

    for col, ind in ind_list.items():
        for indexName in ind:
            create_index2(indexName, table_name, col)
    
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

def indexfilter(metadata, indexhandler, conditions, collection, tablename):
    if conditions is None:
        return [x for x in collection.distinct('_id') if x != 'ඞ']
    matches = condition_pattern.findall(conditions)
    if not matches:
        return False
    result = []
    current_operator = None
    notmytable = True
    for match in matches:
        column, operator, value, logical_op = match
        column = column.strip()
        valami = column.split('.')
        if len(valami) == 2:
            table_name = valami[0]
            column = valami[1]
            #print([table_name, column])
            if tablename != table_name:
                continue
        value = value.strip().strip("'")
        if column not in indexhandler.keys():
            continue
        index = indexhandler[column]
        
        notmytable = False
        if index is None:
            acc = [x for x in collection.distinct('_id') if x != 'ඞ']
        else:
            indexes = [x for x in index.split('ඞ') if x]
            index = metadata.find_one({'_id': indexes[0]})
            index = index_to_dict(index)
            acc = []
            for key, vals in index.items():
                if evaluate_condition(key, operator, value):
                    for x in vals:
                        acc.append(x)
        if len(result) == 0 :
            result = acc.copy()
        elif current_operator == "AND":
            result = list(set(result) & set(acc))
        elif current_operator == "OR":
            result = list(set(result) | set(acc))
        
        current_operator = logical_op.strip().upper() if logical_op else None
    if notmytable: #if no regulations are based on my table then we can return every id
        return [x for x in collection.distinct('_id') if x != 'ඞ']
    return result

def select_table_name_handler(tables, conditions):

    if inner_join_regex_for_select.match('FROM ' + tables):
        match = inner_join_regex_for_select.search('FROM ' + tables)
        table1 = match.group(1)
        table2 = match.group(2)
        col1 = match.group(3)
        col2 = match.group(4)
        rest = match.group(5)
        return inner_join_handler(table1, table2, col1, col2, rest, conditions)
    else:
        if tables not in mydb.list_collection_names():
            return f'{tables} DOES NOT EXIST IN THE CURRENT DATABASE'
        collection = mydb[tables]
        metadata = mydb[f'{tables}.info']
        structorder = metadata.find_one({'_id': 'ඞSTRUCTඞ'})
        struct = backtonormalstruct(structorder)
        index_handler = metadata.find_one({'_id': 'ඞINDEXHANDLERඞ'})
        if struct is None:
            return 'YOUR TABLE WAS MADE IN AN OLDER VERSION'
        pk = struct['KeyValue']
        ids = indexfilter(metadata, index_handler, conditions, collection, tables)
        ret_list = []
        
        for doc in collection.find({'_id': {'$in': ids}}):
            if doc['_id'] != 'ඞ': 
                ans = string_to_dict(backtonormalstring(doc['content'], structorder))
                ans[pk] = doc['_id']
                ret_list.append(ans)
        return ret_list

def select_output_formatting(d: dict) -> str:
    for key, value in d.items():
        d[key] = ', '.join(value)
    return d

def select_all(table_name: str):
    return select_table_name_handler(table_name, None)

def select_col(col_names, table_name):
    doc_list = select_table_name_handler(table_name, None)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    #CHECK IF COL EXISTS
    keys = doc_list[0].keys()
    renames = {}
    for col_name in col_names:
        if 'AS' in col_name:
            parts = col_name.split('AS')
            if len(parts) == 2:
                original_col, alias = parts[0].strip(), parts[1].strip()
                if original_col.strip() not in keys:
                    return f'{original_col} IS AN INVALID COLUMN NAME'
                renames[original_col] = alias
            else:
                raise ValueError(f"Invalid column alias format in: {col_name}")
        else:
            if col_name.strip() not in keys:
                return f'{col_name} IS AN INVALID COLUMN NAME'
            renames[col_name.strip()] = col_name.strip()
    ret_list = []
    for doc in doc_list:
        ret_dict = {}
        for key, alias in renames.items():
            ret_dict[alias] = doc[key]
        ret_list.append(ret_dict)
    return ret_list

def select_all_where(table_name: str, conditions: str):
    doc_list = select_table_name_handler(table_name, conditions)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    ret_list = []
    for doc in doc_list:
        if evaluate_conditions(doc, conditions):
            ret_list.append(doc)
    return ret_list

def select_where(col_names, table_name: str, conditions: str):
    doc_list = select_table_name_handler(table_name, conditions)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    #CHECK IF COL EXISTS
    keys = doc_list[0].keys()
    renames = {}
    for col_name in col_names:
        if 'AS' in col_name:
            parts = col_name.split('AS')
            if len(parts) == 2:
                original_col, alias = parts[0].strip(), parts[1].strip()
                if original_col.strip() not in keys:
                    return f'{original_col} IS AN INVALID COLUMN NAME'
                renames[original_col] = alias
            else:
                raise ValueError(f"Invalid column alias format in: {col_name}")
        else:
            if col_name.strip() not in keys:
                return f'{col_name} IS AN INVALID COLUMN NAME'
            renames[col_name.strip()] = col_name.strip()
    ret_list = []
    for doc in doc_list:
        if evaluate_conditions(doc, conditions):
            ret_dict = {}
            for key, alias in renames.items():
                ret_dict[alias] = doc[key]
            ret_list.append(ret_dict)
    return ret_list

def evaluate_conditions(data, conditions):
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

### AGGREGATE FUNCTIONS ###
#TODO: ezeket nem a selectnel kellene keressuk?
def count_aggregate(col_name: str, table_name: str):
    doc_list = select_table_name_handler(table_name)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    if col_name not in doc_list[0]:
        return f'{col_name} IS AN INVALID COLUMN NAME'
    return len(doc_list)

def sum_aggregate(col_name: str, table_name: str):
    doc_list = select_table_name_handler(table_name)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    if col_name not in doc_list[0]:
        return f'{col_name} IS AN INVALID COLUMN NAME'
    return sum((doc[col_name]) for doc in doc_list)

def avg_aggregate(col_name: str, table_name: str):
    doc_list = select_table_name_handler(table_name)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    if col_name not in doc_list[0]:
        return f'{col_name} IS AN INVALID COLUMN NAME'
    return sum((doc[col_name]) for doc in doc_list) / len(doc_list)

def min_aggregate(col_name: str, table_name: str):
    doc_list = select_table_name_handler(table_name)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    if col_name not in doc_list[0]:
        return f'{col_name} IS AN INVALID COLUMN NAME'
    return min((doc[col_name]) for doc in doc_list)

def max_aggregate(col_name: str, table_name: str):
    doc_list = select_table_name_handler(table_name)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    if col_name not in doc_list[0]:
        return f'{col_name} IS AN INVALID COLUMN NAME'
    return max((doc[col_name]) for doc in doc_list)

def group_by_aggregate(col_name: str, table_name: str, group_by_col: str):
    doc_list = select_table_name_handler(table_name)
    if isinstance(doc_list ,str) or doc_list is None:
        return doc_list
    if col_name not in doc_list[0]:
        return f'{col_name} IS AN INVALID COLUMN NAME'
    if group_by_col not in doc_list[0]:
        return f'{group_by_col} IS AN INVALID COLUMN NAME'
    ret_dict = {}
    for doc in doc_list:
        if doc[group_by_col] not in ret_dict:
            ret_dict[doc[group_by_col]] = []
        ret_dict[doc[group_by_col]].append(doc[col_name])
    return select_output_formatting(ret_dict)

"""
    structorder = metadata.find_one({'_id': 'ඞSTRUCTඞ'})
    struct = backtonormalstruct(structorder)
    backtonormalstring(newstring, structorder)
    doc['content'] helyett backtonormalstring(doc['content'], structorder)
"""