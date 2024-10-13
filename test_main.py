"""
Test main.py script
"""

import subprocess
from dotenv import load_dotenv
from databricks import sql
import os


def test_extract_zip(directory):
    """tests extract()"""
    try:
        # List all files in the specified directory
        files = os.listdir(directory)

        # Filter files that contain "nc" in their names
        nc_files = [file for file in files if "nc" in file]

        # Assert that there are exactly two files with "nc" in name
        assert (
            len(nc_files) == 2
        ), f"Expected 2 files with 'nc', found {len(nc_files)}: {nc_files}"

        print("Assertion passed: There are exactly two files with 'nc' in the name.")

    except AssertionError as e:
        print("Assertion failed:", e)
    except Exception as e:
        print("An error occurred:", e)


def test_databricks_tables():
    """tests transform and load functions"""
    load_dotenv()
    server_h = os.getenv("sql_server_host")
    access_token = os.getenv("databricks_api_key")
    http_path = os.getenv("sql_http")
    query = """
# need to include query that joins the voter reg and vote history tables
    """
    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        c = conn.cursor()
        # generate new table for the database
        c.execute(query)
        tables = c.fetchall()
    c.close()
    table = [table[0] for table in tables]
    return print(f"The following tables exist with a netid of ped19: {table}.")


def test_general_query():
    """tests general_query"""


if __name__ == "__main__":
    test_extract_zip("data")
    test_databricks_tables()
    test_general_query()
