# **Miles Martinez Submission**

# Project Summary

This project consists of ingestion, transformation, storage, and analytics of Spotify data. 20 of my favorite artists have been picked and their data is pulled via the Spotify API in Python. Data includes artist info, albums, and songs. This data has been stored in a SQLite database. Views have been built ontop to make it useful for analytics. Finally, visualizations have been developed using the Seaborn library in Python.

Tools Used: Python, SQLite3, Spotipy, Pandas, Seaborn

# Design

## ETL Pipeline

The ETL pipeline conducts the following 3 things to build the database:

1. Spotify data is extracted via the Spotify API. Data is retreived in JSON format and converted into Pandas dataframes, which I refer to as raw dataframes.
2. The raw dataframes are cleaned and formated in a way that fits the schema requirements. The Pandas library is used for these transformations.
3. The newly cleaned dataframes are inserted into their respective tables in the SQLite database.
4. Views have also been built joining and aggregating these tables.

## Files

- `sql_queries.py` - contains all SQL queries used in this project.
- `create_database.py` - builds the SQLite database along with all tables and views. Calls and executes the queries in _sql_queries.py_
- `etl.py` - runs the entire ETL pipline described above.
- `spotify.db` - The SQLite database.
- `visualization.ipynb` - The notebook used for building the visualations.
- `visualization.pdf` - Contains the visualizations built in `visualization.ipynb`.
- `run.py` - Runs the necessary files in order to complete the project via subprocess. It first executes _create_database.py_ followed by _etl.py_.

# How to Run

```sh
$ python run.py
```

or

```sh
$ python create_database.py
$ python etl.py
```

# Ways to improve

1. Create separate functions for more comprehensive validation checks.
2. Verify and enforce schema should the Spotify API change. Alert if so.
3. Create staging tables for storing raw, unprocessed data.
4. Include RAW_JSON columns to give analysts/scientists the option to self parse.
5. Include LOAD_ID or LOAD_TS columns
