"""
Transform and load with SQlite3 database
"""

import sqlite3
import csv


def load(dataset="./data/pollingplaces_2020.csv"):
    data = open(dataset, newline="", encoding="utf-16")
    # NCSBE data includes null bytes, which must be removed
    payload = csv.reader((line.replace("\0", "") for line in data), delimiter="\t")
    conn = sqlite3.connect("pollingplaces_2020.db")
    c = conn.cursor()
    # generate new table for the database
    c.execute("DROP TABLE IF EXISTS pollingplaces_2020")
    c.execute(
        """
            CREATE TABLE pollingplaces_2020 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            election_dt DATE,
            county_name TEXT, 
            polling_place_id INTEGER,
            polling_place_name TEXT,
            precinct_name TEXT,
            house_num INTEGER,
            street_name TEXT, 
            city TEXT,
            state TEXT,
            zip TEXT)
            """
    )
    # insert values
    c.executemany(
        """
            INSERT INTO pollingplaces_2020 (
            election_dt, 
            county_name,
            polling_place_id,
            polling_place_name,
            precinct_name, 
            house_num,
            street_name,
            city,
            state,
            zip)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
            """,
        payload,
    )
    conn.commit()
    conn.close()
    return "pollingplaces_2020.db"


if __name__ == "__main__":
    load()
