from flask import Flask, request, jsonify

import lib
import re

create_database_regex = re.compile(r'Create Database (\w+)', re.IGNORECASE)
drop_database_regex = re.compile(r'Drop Database (\w+)', re.IGNORECASE)
create_table_regex = re.compile(r'Create Table (\w+)', re.IGNORECASE)
drop_table_regex = re.compile(r'Drop Table (\w+)', re.IGNORECASE)
create_index_regex = re.compile(r'CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+) \((\w+)\)', re.IGNORECASE)
insert_test = re.compile(r'INSERT INTO (\w+)', re.IGNORECASE)
insert_doc_regex = re.compile(r'INSERT INTO ([A-Za-z0-9_]+) \(([^)]*)\)\s+VALUES \(([^)]*)\);', re.IGNORECASE)
delete_doc_regex = re.compile(r'DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(\w+)',re.IGNORECASE)

app = Flask(__name__,
            static_url_path="", 
            static_folder='static')


@app.route("/api/database/commands", methods=['POST'])
def get_databases():
    valami = request.json
    return sch(valami['text'])

def sch(command: str):
    commandMsg = "Command not known"
    if(command == 'trust'):
        lib.trust()
        return 'you trusted'
    if create_database_regex.match(command):
        valami = create_database_regex.search(command)
        commandMsg = lib.create_database(valami.group(1))
    elif drop_database_regex.match(command):
        valami = drop_database_regex.search(command)
        commandMsg = lib.drop_database(valami.group(1))
    elif create_table_regex.match(command):
        valami = create_table_regex.search(command)
        commandMsg = lib.create_table(valami.group(1),command)
    elif drop_table_regex.match(command):
        valami = drop_table_regex.search(command)
        commandMsg = lib.drop_table(valami.group(1))
    elif create_index_regex.match(command):
        match = create_index_regex.search(command)
        index_name = match.group(1)
        table_name = match.group(2)
        columns = match.group(3).split(', ')
        commandMsg = lib.create_index2(index_name, table_name, columns)
    elif insert_doc_regex.match(command):
        commandMsg = 'koszi'
        match = insert_doc_regex.search(command)
        table_name = match.group(1)
        columns = match.group(2).split(', ')
        values = match.group(3).split(', ')
        commandMsg = lib.insertDoc(table_name, columns, values)
    elif delete_doc_regex.match(command):
        print('jaja')
        match = delete_doc_regex.search(command)
        table_name = match.group(1).strip()
        col_name = match.group(2).strip()
        val = match.group(3).strip()
        commandMsg = lib.delete_doc_exact(table_name, col_name, val)
        
    print(commandMsg)
    return commandMsg
        
@app.route("/api/database/db_list", methods=['GET'])
def send_databases():
    databases = lib.list_databases()
    return jsonify(databases)

@app.route("/api/database/select_db", methods=['POST']) 
def select_curr_database():
    name = request.json
    lib.select_curr_database(name['curr_db'])
    return ""


@app.route("/api/table/table_list", methods=['GET'])
def send_tables():
    tables = lib.list_tables()
    return jsonify(tables)