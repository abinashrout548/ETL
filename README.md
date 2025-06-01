# ETL

This repository contains a Python ETL (Extract, Transform, Load) pipeline that processes student data from a CSV file, categorizes students based on their marks, assigns grades, saves the processed data to CSV files, and loads the data into an SQLite database. The ETL job is automated to run daily at a scheduled time using the `schedule` library.

## Features

- Extract student data from CSV
- Transform data: determine pass/fail and assign grades
- Load data into CSV files and SQLite database
- Automate ETL execution daily at 2:10 AM

## Installation

Make sure you have Python 3.x installed. Install required packages with:

``bash
pip install pandas schedule
---
## Author
Abinash Rout — https://github.com/abinashrout548
