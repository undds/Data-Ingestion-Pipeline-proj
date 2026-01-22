Data Ingestion subsystem - pt1

# 5 Week Capstone

## Part 1 - Data Ingestion Subsystem

### Application Overview

The Data Ingestion Subsystem is designed to collect and organize data from different sources
into a single place. This part of the system — the Data Ingestion Subsystem — uses Python and
PostgreSQL to read data, clean it, and load it into a database for later use in analytics.

It will:

- Read data from files (CSV or JSON).
- Check and clean the data for accuracy.
- Store valid data in PostgreSQL tables for future processing.
- Keep logs and reports for each data load.

### Core Functional Scope

#### As a data engineer, I want to

- Read data from different sources (CSV, JSON).
- Validate that the data matches the correct format and structure.
- Clean and standardize the data (for example, fix missing or wrong values).
- Remove duplicate records.
- Load the cleaned data into PostgreSQL tables.
- Keep track of any errors or rejected records for review.

#### Main Database Tables

- stg_sales – stores sales transaction data.(Use an appropriate name for your data set)
- stg_rejects – stores records that failed validation.

#### Standard Functional Scope

The Data Ingestion application will:

1. Be written in Python and connect to a PostgreSQL database.
2. Use configuration files (like YAML or JSON or ENV) to define data sources and settings.
3. Handle errors properly and continue with valid records.
4. Allow new data sources to be added easily without major code changes.

### Definition of Done

The Data Ingestion Subsystem will be considered complete when:
• It can successfully read and load data from CSV and JSON files into PostgreSQL.
• At least 80% of the code is tested with PyTest.
• Database connections close properly after use.
• A short demo and code repository are shared for review.

### Non-Functional Expectations

- The design should be simple and modular, making future updates easy.
- Follow standard naming, formatting, and version control practices.
- Use parameterized queries to prevent SQL injection.

###Possible DB options
- Host on AWS (free): https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_GettingStarted.CreatingConnecting.PostgreSQL.html
	- Turn off autoscaling!!
- https://aiven.io/ (offers free PostgreSQL DB)
- https://www.kamatera.com/services/postgresql/ (free for 30 days, but requires a credit card)
- https://www.elephantsqldb.com/try-elephantsqldb (now only offers 7 day free trial)

###DataSet
- https://data.gov/