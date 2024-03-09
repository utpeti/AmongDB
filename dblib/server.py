from socket import *
import lib
import re

### REGEX: ###

create_database_regex = re.compile(r'Create Database (\w+)', re.IGNORECASE)
drop_database_regex = re.compile(r'Drop Database (\w+)', re.IGNORECASE)
create_table_regex = re.compile(r'Create Table (\w+)', re.IGNORECASE)
drop_table_regex = re.compile(r'Drop Table (\w+)', re.IGNORECASE)

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
        lib.create_table(valami.group(1))
    elif drop_table_regex.match(command):
        valami = drop_table_regex.search(command)
        lib.drop_table(valami.group(1))
    else:
        print("Command not known")