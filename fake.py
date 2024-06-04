import random

from faker import Faker


# Initialize Faker

fake = Faker()


# Define the function to generate a single INSERT statement

def generate_insert_statement(n):

    # Generate a random student ID

    stud_id = n


    # Generate a fake name

    name = fake.first_name() + "_"  + fake.last_name()


    # Generate a fake birthday

    birthday = fake.date_of_birth(minimum_age=18, maximum_age=50).strftime("%Y/%m/%d")


    # Generate a random average grade between 0 and 10

    avg = round(random.uniform(0, 10), 1)


    # Return the INSERT statement as a string

    return f"INSERT INTO STUDENTS (stud_id, name, birthday, avg) VALUES ({stud_id}, {name}, {birthday}, {avg});\n"


# Create a new text file to write the INSERT statements

with open("insert_statements.txt", "w") as f:

    for i in range(1, 100000):

        f.write(generate_insert_statement(i))