 # This is a README for IDS 706 Mini Project 5

### Status Badge:
[![CICD](https://github.com/nogibjj/PeterdeGuzman_Mini5/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/PeterdeGuzman_Mini5/actions/workflows/cicd.yml)

### Summary and Project Motivation:
In this project, I built a pipeline that connects a publicly available CSV to a SQLite database and executes Create, Read, Update, and Delete (CRUD) operations using SQL syntax in a Python script. These operations could be used in the future to update entries in the database. In this example, each database entry represents the address of a polling place location. 

### Data Used in this Project:
This project uses data on Election Day polling places for the November 3rd, 2020 election in North Carolina.

More information and a link to the data is available at: https://www.ncsbe.gov/results-data/polling-place-data

### Structure:
- In the `mylib` directory, `extract.py` extracts the raw data from the link to the NCSBE website. The `transform_load.py` script transforms the raw data from `.csv` to a `.db` SQLite database and creates a new connection.
- The `query.py` script includes the Create, Read, Update, and Delete (CRUD) operations to execute various SQL operations. 

### Proof of Successful Database Operations
- Please view the "main.ipynb" notebook to view proof of successful database operations. 

### Test:
![alttext](proof_test.png)


### Project Directory:
```
PeterdeGuzman_Mini5/
├── __pycache__/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .pytestcache/
├── .ruff_cache/
├── __pycache__
├── .github/
│   └── workflows/
│       └── cicd.yml
├── mylib/
│      ├── extract.py
│      ├── transform_load.py
│      └── query.py
├── data/
│       └── pollingplaces_2020.csv
├── .gitignore
├── proof_test.png
├── pllingplaces_2020.db
├── main.py
├── main.ipynb
├── main.html
├── main.pdf
├── Makefile
├── README.md
├── Requirements.txt
└── test_main.py
```


