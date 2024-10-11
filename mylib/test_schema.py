import sqlite3


# Function to print the schema of the SQLite database
def print_schema(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)

    # Create a cursor object
    cursor = conn.cursor()

    # Query to get the schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Schema for table: {table_name}")

        # Get the schema of the table
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()

        # Print the schema details
        for column in schema:
            print(
                f"Column: {column[1]}, Type: {column[2]}, Not Null: {column[3]}, Default: {column[4]}, Primary Key: {column[5]}"
            )

        print("\n" + "=" * 40 + "\n")

    # Close the connection
    conn.close()


# Specify the path to your SQLite database
db_file = "/Users/pdeguz01/Documents/git/PeterdeGuzman_Mini6/voterreg_2024.db"
print_schema(db_file)
db_file2 = "/Users/pdeguz01/Documents/git/PeterdeGuzman_Mini6/pollingplaces_2020.db"
print_schema(db_file2)
