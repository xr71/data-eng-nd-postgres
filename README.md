# Postgres Star Schema Design Project for Udacity Data Engineering Nanodegree

To optimize analytical and downstream queries by building a star schema for searching through songs played by online users. 

## Database Design
* 4 dimensional tables
  * Users
  * Artists
  * Songs
  * Time
* 1 Facts table (de-normalized)
  * Songplays
  
Build by combining raw JSON files from a event logs and playlist information. 

## Instructions
* Run `python create_tables` followed by `python etl.py` to load all of the raw files into your Postgres environment.
* Please take a look at `etl.ipynb` if you would like to understand the ETL Pipeline process.
* Please take a look at `sql_queries.py` if you would like to understand the DDL schematics and how the tables are defined and subsequently how data are inserted.
* OPTIONAL: please take a look at `analytics_demo.ipynb` if you would like to see an example of running statistical or analytical queries against the database.
