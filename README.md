# Student ETL Pipeline with Scheduling

This project implements a simple ETL (Extract, Transform, Load) pipeline for student data using Python. It reads student data from a CSV file, processes it to determine pass/fail status and grades, saves the results to new CSV files, and loads the data into an SQLite database. The ETL job is scheduled to run automatically every day at 2:10 AM.

---

## Features

- Reads student data from CSV
- Calculates pass/fail status and grades
- Saves processed data to separate CSV files for passed and failed students
- Loads data into an SQLite database (`students.db`)
- Automates ETL execution using the `schedule` library

---
## Requirements

- Python 3.x
- pandas
- schedule
- sqlite3 (standard Python library)

Install required packages using pip:

``bash
pip install pandas schedule
---
## Author
Abinash Rout — abinashrout584@gmail.com
