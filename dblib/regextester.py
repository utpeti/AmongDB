import lib
import re

create_database_regex = re.compile(r'Create Database (\w+)', re.IGNORECASE)
drop_database_regex = re.compile(r'Drop Database (\w+)', re.IGNORECASE)
create_table_regex = re.compile(r'Create Table (\w+)', re.IGNORECASE)
drop_table_regex = re.compile(r'Drop Table (\w+)', re.IGNORECASE)

#REGEX TESTER#

def read_command():
    contents = []
    while True:
        line = input()
        contents.append(line)

        if line.endswith(';'):
            break
    command_str = " ".join(contents)
    return command_str

while True:
    command = read_command()  
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