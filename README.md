# Multinational-retail-data-centralisation

## Table of Content
1. [Project Description](#project-description)
    - [Overview](#overview)
    - [Prerequisites](#prerequisites)
    - [Dependencies](#dependencies)
1. [Installation Instruction](#installation-instruction)
    - [Configuration files](#configuration-files)
    - [Virtual Environment](#virtual-environment)
    - [Database](#database)
1. [Usage Instruction](#usage-instruction)
    - [ETL process](#etl-process)
    - [Building a star schema](#building-a-star-schema)
    - [Query the data](#query-the-data)
    - [Run the code](#run-the-code)
    - [Testing](#testing)
1. [File Structure](#file-structure)
1. [License](#license)

## Project Description

### Overview
The Multinational Retail Data Centralisation project focuses on data engineering tasks, including the extraction, cleaning, and loading of data from diverse sources within a multinational retail organization. The primary goal is to create a star-based schema and address business-related inquiries through SQL queries. The project encompasses the extraction of data from various sources such as AWS RDS, web APIs, PDF files, S3 buckets, and JSON files. The data cleaning stage utilizes the Pandas Python package following best practices. All data cleaning functionality is abstracted in a DataCleaning class. Methods that involve single responsiblity in the DataCleaning class through test-driven-devolpment TDD using pytest package. The project utilises makfile to automate processes involved, include ETL process, building the db schema and quering the data. The code is written in compliance with the PEP 8 style guide using flake8 linter.

### Prerequisites
- python 3+
- pip
- posgresql
- AWS CLI configured with aws iam user credentials
- java 8+
- make (optional)

### Dependencies
- Pandas
- QLAlchemy
- boto3
- tabula-py
  
For full details, see [***requirements.txt***](requirements.txt)


## Installation Instruction

### Configuration files

The project requires 3 configuration files as follow:

#### db_creds.yaml

This file should contain aicore AWS RDS credentials as follow:

```yaml
    RDS_HOST: "<aicore-amazon-rds-endpoint>"
    RDS_PASSWORD:  "<aicore-amazon-rds-password>"
    RDS_USER: "postgres"
    RDS_DATABASE: "postgres"
    RDS_PORT: "5432"
```

#### local_db_creds.yaml

This file should contain database credentials for the database that is used to load the data, the file should look as follow:

```yaml
    HOST: "<host>"
    PASSWORD: "<password>"
    USER: "<user>"
    DATABASE: "sales_data"
    PORT: "<port>"
```

#### .env
```dotenv
    X-API-KEY="<x-api-key for stores data api header>"
```

### Virtual Environment

The project requires a virtual environment setup with all dependicies.  

To create virtual environment (**venv**), from CLI run:
```bash
    make create-venv
```

Alternatively, run:
```bash
    python -m venv venv
```

To install project dependicies, from CLI run:
```bash
    make install-req
```

Alternatively, from within **venv** run:
```bash
    pip install -r requirements.txt
```

### Database

The project requires **sales_data** database to load cleaned data.

To set-up data, from CLI run:
```bash
    make setup-db
```

Alternatively, run:
```
    psql -f ./db/db-setup.sql
```

## Usage Instruction
Note: The make commands defined below are specifically crafted to execute automatically within the virtual environment. Consequently, there is no necessity to manually activate the virtual environment each time. To manually activate virtual environment, simply type:
```bash
    source venv/bin/activate
```

### ETL process

To run data extract, clean and load process (ETL), from CLI run:
```bash
    make run-etl
```

Alternatively, run main.py script as follow:
```
    python ./src/main.py
```

### Building a star schema

To build star schema, from CLI run:
```bash
    make build-db-schema
```

Alternatively, run create_db_schem.sql script as follow:
```
    psql -f db/create_db_schema.sql
```

### Query the data

To retrieve business related info, from CLI run:
```bash
    make query-the-data
```

Alternatively, run query_the_data.sql script as follow:
```
    psql -f ./db/query_the_data.sql
```

### Run the code

To run the whole process at once, from CLI run:
```bash
    make run-code
```

Alternatively, from within **venv** run:
```bash
    psql -f ./db/db-setup.sql && python ./src/main.py \
    psql -f db/create_db_schema.sql && psql -f ./db/query_the_data.sql
```

### Testing
To run all pytest tests built during development process and flake8 linter checks, from CLI run:
```
    make run-checks
```

Alternatively, from within virtual environment run:
```bash
    PYTHONPATH=$(pwd) pytest -v && \
    flake8 ./src/*.py \
	./test/test_data_extraction/*.py \
	./test/test_data_cleaning/*.py
```

## File Structure
```zsh
.
├── Makefile
├── README.md
├── db
│   ├── create_db_schema.sql
│   ├── db-setup.sql
│   └── query_the_data.sql
├── db_creds.yaml
├── local_db_creds.yaml
├── requirements.txt
├── setup.cfg
├── src
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   ├── database_utils.py
│   └── main.py
└── test
    ├── test_data_cleaning
    │   ├── test_assign_valid_country_code.py
    │   ├── test_clean_card_number.py
    │   ├── test_convert_product_weights.py
    │   ├── test_convert_to_kg.py
    │   ├── test_fix_date_format.py
    │   ├── test_is_invalid_data_point.py
    │   ├── test_remove_alpha_letters_from_staff_number.py
    │   └── test_replace_null_with_nan.py
    └── test_data_extraction
        └── test_parse_s3_address.py
```
## License
