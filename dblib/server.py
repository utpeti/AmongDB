from socket import *
import lib
import re

### REGEX: ###

create_database_regex = re.compile(r'Create Database (\w+)', re.IGNORECASE)
drop_database_regex = re.compile(r'Drop Database (\w+)', re.IGNORECASE)
create_table_regex = re.compile(r'Create Table (\w+)', re.IGNORECASE)
drop_table_regex = re.compile(r'Drop Table (\w+)', re.IGNORECASE)
create_index_regex = re.compile(r'CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)', re.IGNORECASE)

### SERVER: ###

serverPort = 3000
serverSocket = socket(AF_INET, SOCK_STREAM)
serverSocket.bind(('', serverPort)) 
serverSocket.listen(1)
print("SERVER: Ready to receive")

while True:
    connectionSocket, addr = serverSocket.accept()
    command = connectionSocket.recv(1024).decode()
    connectionSocket.close()
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
    elif create_index_regex.match(command):
        match = create_index_regex.search(command)
        index_name = match.group(1)
        table_name = match.group(2)
        columns = match.group(3).split(', ')
        lib.create_index(index_name, table_name, columns)
    else:
        print("Command not known")