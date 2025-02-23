# Example ETL usage

## Introduction

This example performs the following:
- Retrieves source XLSX document from ONS
- Parse XLSX document and produce:
  - Single CSV
  - Separate model CSV - for use with a relational database
  - Python dictionary file - used by search service

## Usage

Install dependencies with:
```shell
pip install -r requirements.txt
```

Run example with:
```shell
python app.py
```

Examine the contents of the `output` directory for results.
