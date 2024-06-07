import random

from faker import Faker
from pymongo import MongoClient
cluster = MongoClient('mongodb+srv://korposb:1234@amongdb.xrci9ew.mongodb.net/?retryWrites=true&w=majority&appName=AmongDB')
mydb = cluster["bemutato_teszt"]
table = mydb['STUDENTS']
table2 = mydb['PERSONAL_INFORMATION']

# Initialize Faker

fake = Faker()


# Define the function to generate a single INSERT statement

def generate_insert_statement(n):

    # Generate a random student ID

    stud_id = 2*n


    # Generate a fake name

    name = fake.first_name() + "_"  + fake.last_name()


    # Generate a fake birthday

    birthday = fake.date_of_birth(minimum_age=18, maximum_age=50).strftime("%Y/%m/%d")


    # Generate a random average grade between 0 and 10

    avg = round(random.uniform(0, 10), 1)
    phone_number = fake.phone_number()
    postal_code = fake.postcode()
    email = fake.email()
    address = fake.address()

    # Return the INSERT statement as a string
    return [f"{name}#{birthday}#{avg}", f'{name}#{phone_number}#{postal_code}#{email}#{address}']


# Create a new text file to write the INSERT statements
documents1 = []
documents2 = []

for i in range(1, 100000):
    contents = generate_insert_statement(i)
    documents1.append({'_id': f'{i}', 'content': contents[0]})
    documents2.append({'_id': f'{2*i}', 'content': contents[1]})

table.insert_many(documents1)
table2.insert_many(documents2)