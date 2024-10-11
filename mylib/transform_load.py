"""
Transform and load with SQlite3 database
"""

from databricks import sql
import csv
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

csv.field_size_limit(100000000)

# set cwd
main_directory = "/Users/pdeguz01/Documents/git/PeterdeGuzman_Mini6/"  # Change this to your desired path
os.chdir(main_directory)

# Transform functions
# might need to create two different transforms
# voter reg encoding is Windows-1252
# vote history encoding is ascii


def transform_voterreg(txtfile, county, date, directory):
    # Transforming voter reg data from encoding="Windows-1252" to encoding="utf-16"
    df = pd.read_csv(
        txtfile,
        delimiter="\t",
        encoding="windows-1252",
        on_bad_lines="skip",
        low_memory=False,
    )

    # Save as UTF-16 encoded CSV
    filepath = os.path.join(directory, f"voterreg_{county}{date}.csv")

    df.to_csv(
        filepath,
        sep="\t",
        encoding="utf-16",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
    )


def transform_votehistory(txtfile, county, date, directory):
    # Transforming voter history data from encoding="ascii" to encoding="utf-16"
    df = pd.read_csv(
        txtfile,
        delimiter="\t",
        encoding="ascii",
        on_bad_lines="skip",
        low_memory=False,
    )

    # Save as UTF-16 encoded CSV
    filepath = os.path.join(directory, f"votehist_{county}{date}.csv")

    df.to_csv(
        filepath,
        sep="\t",
        encoding="utf-16",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
    )


def load_voterreg(netid, dataset, date):
    """Loads data into the databricks database"""
    db_name = "voterreg_"
    payload = csv.reader(
        open(dataset, encoding="utf-16"),
        delimiter="\t",
    )
    load_dotenv()
    server_h = os.getenv("sql_server_host")
    access_token = os.getenv("databricks_api_key")
    http_path = os.getenv("sql_http")
    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        c = conn.cursor()
        # generate new table for the database
        c.execute(f"SHOW TABLES FROM default LIKE 'ped19*'")
        result = c.fetchall()
        if not result:
            c.execute(
                f"""
            CREATE TABLE IF NOT EXISTS{netid}{db_name}{date} (
            county_id INT,
            county_desc STRING,
            voter_reg_num STRING,
            ncid STRING,
            last_name STRING,
            first_name STRING,
            middle_name STRING,
            name_suffix_lbl STRING,
            status_cd STRING,
            voter_status_desc STRING,
            reason_cd STRING,
            voter_status_reason_desc STRING,
            res_street_address STRING,
            res_city_desc STRING,
            state_cd STRING,
            zip_code STRING,
            mail_addr1 STRING,
            mail_addr2 STRING,
            mail_addr3 STRING,
            mail_addr4 STRING,
            mail_city STRING,
            mail_state STRING,
            mail_zipcode STRING,
            full_phone_number STRING,
            confidential_ind STRING,
            regisr_dt STRING,
            race_code STRING,
            ethnic_code STRING,
            party_cd STRING,
            gender_code STRING,
            birth_year INT,
            age_at_year_end STRING,
            birth_state STRING,
            drivers_lic STRING,
            precinct_abbrv STRING,
            precinct_desc STRING,
            municipality_abbrv STRING,
            municipality_desc STRING,
            ward_abbrv STRING,
            ward_desc STRING,
            cong_dist_abbrv STRING,
            super_court_abbrv STRING,
            judic_dist_abbrv STRING,
            nc_senate_abbrv STRING,
            nc_house_abbrv STRING,
            county_commiss_abbrv STRING,
            county_commiss_desc STRING,
            township_abbrv STRING,
            township_desc STRING,
            school_dist_abbrv STRING,
            school_dist_desc STRING,
            fire_dist_abbrv STRING,
            fire_dist_desc STRING,
            water_dist_abbrv STRING,
            water_dist_desc STRING,
            sewer_dist_abbrv STRING,
            sewer_dist_desc STRING,
            sanit_dist_abbrv STRING,
            sanit_dist_desc STRING,
            rescue_dist_abbrv STRING,
            rescue_dist_desc STRING,
            munic_dist_abbrv STRING,
            munic_dist_desc STRING,
            dist_1_abbrv STRING,
            dist_1_desc STRING,
            vtd_abbrv STRING,
            vtd_description STRING)
            """
            )
            # insert
            for _, row in payload.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO {netid}{db_name}{date} VALUES {convert}")
        c.close()
    return "success"


# update table columns and values
def load_votehistory(netid, dataset, date):
    db_name = "votehist_"
    payload = csv.reader(
        open(dataset, encoding="utf-16"),
        delimiter="\t",
    )
    load_dotenv()
    server_h = os.getenv("sql_server_host")
    access_token = os.getenv("databricks_api_key")
    http_path = os.getenv("sql_http")
    with sql.connect(
        server_hostname=server_h,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        c = conn.cursor()
        # generate new table for the database
        c.execute(f"SHOW TABLES FROM default LIKE '{netid}*'")
        result = c.fetchall()
        if not result:
            c.execute(
                f"""
            CREATE TABLE {netid}{db_name}{date} (
            county_id INT,
            county_desc STRING,
            voter_reg_num STRING,
            election_lbl STRING,
            election_desc STRING,
            voting_method STRING,
            voted_party_cd STRING,
            voted_party_desc STRING,
            pct_label STRING,
            pct_description STRING,
            ncid STRING,
            voted_county_id STRING,
            voted_county_desc STRING,
            vtd_label STRING,
            vtd_description STRING)
            """
            )
            # insert
            for _, row in payload.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO {netid}{db_name}{date} VALUES {convert}")
        c.close()
    return "success"


# def load_pollingplaces(dataset, year):
#     data = open(dataset, newline="", encoding="utf-16")
#     # NCSBE data includes null bytes, which must be removed
#     payload = csv.reader((line.replace("\0", "") for line in data), delimiter="\t")
#     db_name = "pollingplaces_"
#     conn = sqlite3.connect(f"{db_name}{year}.db")
#     c = conn.cursor()
#     # generate new table for the database
#     c.execute(f"DROP TABLE IF EXISTS {db_name}{year}")
#     c.execute(
#         f"""
#             CREATE TABLE {db_name}{year} (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             election_dt DATE,
#             county_name TEXT,
#             polling_place_id INTEGER,
#             polling_place_name TEXT,
#             precinct_name TEXT,
#             house_num INTEGER,
#             street_name TEXT,
#             city TEXT,
#             state TEXT,
#             zip TEXT)
#             """
#     )
#     # insert values
#     c.executemany(
#         f"""
#             INSERT INTO {db_name}{year} (
#             election_dt,
#             county_name,
#             polling_place_id,
#             polling_place_name,
#             precinct_name,
#             house_num,
#             street_name,
#             city,
#             state,
#             zip)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """,
#         payload,
#     )
#     conn.commit()
#     conn.close()
#     return f"{db_name}{year}.db"


if __name__ == "__main__":
    today = datetime.now()
    transform_voterreg(
        txtfile=f"{main_directory}/data/ncvoter32.txt",
        county="Durham",
        date="241011",
        directory="data",
    )
    transform_votehistory(
        txtfile=f"{main_directory}/data/ncvhis32.txt",
        county="Durham",
        date="241011",
        directory="data",
    )
    # load_pollingplaces(
    #     dataset=f"{main_directory}/data/pollingplaces_2020.csv", year=2020
    # )
    load_voterreg(
        netid="ped19",
        dataset=f"{main_directory}/data/voterreg_Durham241011.csv",
        date=today.strftime("%Y_%m_%d"),
    )
    load_votehistory(
        netid="ped19",
        dataset=f"{main_directory}/data/votehist_Durham241011.csv",
        date=today.strftime("%Y_%m_%d"),
    )
