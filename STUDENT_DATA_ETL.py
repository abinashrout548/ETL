#!/usr/bin/env python
# coding: utf-8

# # Exercise 1: Create a Student Class

# 
# Level: Easy
# Concepts: Class, Constructor, Method
# 
# 🔧 Task:
# Create a Student class with the following:
# 
# Attributes: name, roll_no, marks
# 
# Method to display student info
# 
# Method to check if the student passed (pass if marks ≥ 40)

# # ETL
# # 1. Extract

# In[4]:


import pandas as pd
class Student:
    def __init__(self,name, roll_no, marks):
        self.name=name
        self.roll_no=roll_no
        self.marks=marks
    def display(self):
        print(f"Student Name: {self.name}.")
        print(f"Student roll_no: {self.roll_no}.")
        print(f"Student marks: {self.marks}.")

    def is_passed(self):
        return self.marks >= 40
        '''if self.marks >= 40:
            print(f"The student {self.name} is passed")
            return True
        else:
            print(f"The student {self.name} is not passed")
            return False '''


df = pd.read_csv("C:\\DataScience\\DATA\\student.csv")

passed_students=[]
failed_students= []

for index, row in df.iterrows():
    s = Student(row['name'], row['roll_no'], row['marks'])
    
    if s.is_passed():
        passed_students.append(s)
    else:
        failed_students.append(s)

for student in passed_students:
    print(f"{student.name} (Roll No: {student.roll_no}) passed with {student.marks} marks.")
print("-" * 50)
for student in failed_students:
    print(f"{student.name} (Roll No: {student.roll_no}) failed with {student.marks} marks.")


# # 2. Transform

# In[ ]:


import pandas as pd
class Student:
    def __init__(self,name, roll_no, marks):
        self.name=name
        self.roll_no=roll_no
        self.marks=marks
    def display(self):
        print(f"Student Name: {self.name}.")
        print(f"Student roll_no: {self.roll_no}.")
        print(f"Student marks: {self.marks}.")

    def is_passed(self):
        return self.marks >= 40
        '''if self.marks >= 40:
            print(f"The student {self.name} is passed")
            return True
        else:
            print(f"The student {self.name} is not passed")
            return False '''

    def grade(self):
        if self.marks >=90:
            return 'A'
        elif self.marks >=80:
            return 'B'
        elif self.marks >=70:
            return 'C'
        elif self.marks >=60:
            return 'D'
        elif self.marks >=50:
            return 'E'
        else:
            return 'F'

df = pd.read_csv("C:\\DataScience\\DATA\\student.csv")

passed_students=[]
failed_students= []

students_data=[]

for index, row in df.iterrows():
    s = Student(row['name'], row['roll_no'], row['marks'])
    students_data.append({
        'name':s.name,
        'roll_no':s.roll_no,
        'marks':s.marks,
        'passed':s.is_passed(),
        'grade':s.grade()
    })
transformed_df=pd.DataFrame(students_data)

passed_students_df=transformed_df[transformed_df['passed'] == True]
failed_students_df=transformed_df[transformed_df['passed'] == False]

## Loa To as CSV to Local Machine.

passed_students_df.to_csv(#"C:\\DataScience\\DATA\\passed_students.csv", index=False)
failed_students_df.to_csv(#"C:\\DataScience\\DATA\\failed_students.csv", index=False)

print("ETL process completed. Files saved: passed_students.csv, failed_students.csv")


# In[12]:


passed_students_df.head()


# # 3. Automated Load To Local Machine

# In[ ]:


import schedule
import time
import pandas as pd

class Student:
    def __init__(self,name, roll_no, marks):
        self.name=name
        self.roll_no=roll_no
        self.marks=marks
    def display(self):
        print(f"Student Name: {self.name}.")
        print(f"Student roll_no: {self.roll_no}.")
        print(f"Student marks: {self.marks}.")

    def is_passed(self):
        return self.marks >= 40
        '''if self.marks >= 40:
            print(f"The student {self.name} is passed")
            return True
        else:
            print(f"The student {self.name} is not passed")
            return False '''

    def grade(self):
        if self.marks >=90:
            return 'A'
        elif self.marks >=80:
            return 'B'
        elif self.marks >=70:
            return 'C'
        elif self.marks >=60:
            return 'D'
        elif self.marks >=50:
            return 'E'
        else:
            return 'F'
def run_etl():
    print("Running ETL job...")
    # Your ETL code here, or import and call a function
    df = pd.read_csv("C:\\DataScience\\DATA\\student.csv")

    # TRANSFORM DATA
    passed_students=[]
    failed_students= []

    students_data=[]

    for index, row in df.iterrows():
        s = Student(row['name'], row['roll_no'], row['marks'])
        students_data.append({
            'name':s.name,
            'roll_no':s.roll_no,
            'marks':s.marks,
            'passed':s.is_passed(),
            'grade':s.grade()
        })
    transformed_df=pd.DataFrame(students_data)

    passed_students_df=transformed_df[transformed_df['passed'] == True]
    failed_students_df=transformed_df[transformed_df['passed'] == False]

    passed_students_df.to_csv("C:\\DataScience\\DATA\\passed_students.csv", index=False)
    failed_students_df.to_csv("C:\\DataScience\\DATA\\failed_students.csv", index=False)

    print("ETL process completed. Files saved: passed_students.csv, failed_students.csv")
    
# SCHEDULE TO RUN DAILY AT 2:00 AM
schedule.every().day.at("02:12").do(run_etl)

print("Scheduler started. Waiting for ETL to run daily at 2:10 AM...")
while True:
    schedule.run_pending()
    time.sleep(60)


# # 4. Load To DataBase

# In[ ]:


import schedule
import time
import pandas as pd
import sqlite3

class Student:
    def __init__(self,name, roll_no, marks):
        self.name=name
        self.roll_no=roll_no
        self.marks=marks
    def display(self):
        print(f"Student Name: {self.name}.")
        print(f"Student roll_no: {self.roll_no}.")
        print(f"Student marks: {self.marks}.")

    def is_passed(self):
        return self.marks >= 40
        '''if self.marks >= 40:
            print(f"The student {self.name} is passed")
            return True
        else:
            print(f"The student {self.name} is not passed")
            return False '''

    def grade(self):
        if self.marks >=90:
            return 'A'
        elif self.marks >=80:
            return 'B'
        elif self.marks >=70:
            return 'C'
        elif self.marks >=60:
            return 'D'
        elif self.marks >=50:
            return 'E'
        else:
            return 'F'
def run_etl():
    print("Running ETL job...")
    # Your ETL code here, or import and call a function
    df = pd.read_csv("C:\\DataScience\\DATA\\student.csv")

    # TRANSFORM DATA
    passed_students=[]
    failed_students= []

    students_data=[]

    for index, row in df.iterrows():
        s = Student(row['name'], row['roll_no'], row['marks'])
        students_data.append({
            'name':s.name,
            'roll_no':s.roll_no,
            'marks':s.marks,
            'passed':s.is_passed(),
            'grade':s.grade()
        })
    transformed_df=pd.DataFrame(students_data)

    passed_students_df=transformed_df[transformed_df['passed'] == True]
    failed_students_df=transformed_df[transformed_df['passed'] == False]

    passed_students_df.to_csv("passed_students.csv", index=False)
    failed_students_df.to_csv("failed_students.csv", index=False)

    print("ETL process completed. Files saved: passed_students.csv, failed_students.csv")

    ## LOAD DATA TO DATABASE

    #import sqlite3

    # Connect to (or create) SQLite database
    conn = sqlite3.connect("students.db")
    cursor = conn.cursor()

    # Create the table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS students (
    roll_no INTEGER PRIMARY KEY,
    name TEXT,
    marks REAL,
    passed BOOLEAN,
    grade TEXT
    )
    ''')

    # Insert or replace rows
    for _, row in transformed_df.iterrows():
        cursor.execute('''
            INSERT OR REPLACE INTO students (roll_no, name, marks, passed, grade)
            VALUES (?, ?, ?, ?, ?)
        ''', (row['roll_no'], row['name'], row['marks'], row['passed'], row['grade']))

    conn.commit()
    conn.close()

    print("Data successfully loaded into students.db.")

# SCHEDULE TO RUN DAILY AT 2:00 AM
schedule.every().day.at("02:07").do(run_etl)

print("Scheduler started. Waiting for ETL to run daily at 2:07 AM...")
while True:
    schedule.run_pending()
    time.sleep(60)


# In[ ]:




