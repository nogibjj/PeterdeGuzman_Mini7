"""
Query the dataset
"""

import sqlite3


def query_create():
    conn = sqlite3.connect("pollingplaces_2020.db")
    cursor = conn.cursor()
    # create query
    cursor.execute(
        """
        INSERT INTO pollingplaces_2020 
        (election_dt,county_name,polling_place_id, polling_place_name, precinct_name, 
        house_num, street_name, city, state,zip) 
        VALUES(11/03/2020, 'DURHAM', 99, 'GROSS HALL', 'DUKE MIDS', 
        140, 'SCIENCE DRIVE', 'DURHAM', 'NC', '27708')
        """
    )
    conn.close()
    return "Create Success"


def query_read():
    conn = sqlite3.connect("pollingplaces_2020.db")
    cursor = conn.cursor()
    # execute read
    cursor.execute("SELECT * FROM pollingplaces_2020 LIMIT 10")
    conn.close()
    return "Read Success"


def query_update():
    conn = sqlite3.connect("pollingplaces_2020.db")
    cursor = conn.cursor()
    # update
    cursor.execute("UPDATE pollingplaces_2020 SET county_name = 'DURHAM' WHERE id = 20")
    conn.close()
    return "Update Success"


def query_delete():
    conn = sqlite3.connect("pollingplaces_2020.db")
    cursor = conn.cursor()
    # delete
    cursor.execute("DELETE FROM pollingplaces_2020 WHERE id = 10")
    conn.close()
    return "Delete Success"


if __name__ == "__main__":
    query_create()
    query_read()
    query_update()
    query_delete()
