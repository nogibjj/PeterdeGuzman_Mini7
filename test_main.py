"""
Test main.py script
"""

import subprocess
from dotenv import load_dotenv
from databricks import sql
import os


def test_extract_zip():
    """tests extract()"""
    result = subprocess.run(
        ["python", "main.py", "extract_zip"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Extracting data..." in result.stdout


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
    result = subprocess.run(
        [
            "python",
            "main.py",
            "general_query",
            """SELECT t1.month, t1.SUM(births)
            FROM default.births2000DB t1 JOIN default.births1994DB t2
            ON t1.year=t2.year
            GROUP BY t1.month
            ORDER BY t1.month
            LIMIT 10
            """,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


if __name__ == "__main__":
    test_cases_extract_zip = [
        ("https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip", "data"),
        ("https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis32.zip", "data"),
    ]
    for url, date in test_cases_extract_zip:
        print(f"Testing with text cases: {url} and date: {date}")
        test_extract_zip(url, date)
    test_databricks_tables()
    test_general_query()
