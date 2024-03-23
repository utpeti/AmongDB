#flask --app ./server2.py --debug run
#ha akarod futtatni is :D

from flask import Flask, request, jsonify

import lib
import re

create_database_regex = re.compile(r'Create Database (\w+)', re.IGNORECASE)
drop_database_regex = re.compile(r'Drop Database (\w+)', re.IGNORECASE)
create_table_regex = re.compile(r'Create Table (\w+)', re.IGNORECASE)
drop_table_regex = re.compile(r'Drop Table (\w+)', re.IGNORECASE)


app = Flask(__name__,
            static_url_path="", 
            static_folder='static')


@app.route("/api/databases", methods=['POST'])
def get_databases():
    valami = request.json
    sch(valami['text'])
    return ""

def sch(command: str):
    if create_database_regex.match(command):
        valami = create_database_regex.search(command)
        lib.create_database(valami.group(1))
    elif drop_database_regex.match(command):
        valami = drop_database_regex.search(command)
        lib.drop_database(valami.group(1))
    elif create_table_regex.match(command):
        valami = create_table_regex.search(command)
        lib.create_table(valami.group(1),command)
    elif drop_table_regex.match(command):
        valami = drop_table_regex.search(command)
        lib.drop_table(valami.group(1))
    else:
        print("Command not known")
        
'''
CREATE TABLE LAJOS (
    ID INT,
    f FLOAT,
    b BIT,
    d DATE,
    dt DATETIME,
    name VARCHAR
);
'''