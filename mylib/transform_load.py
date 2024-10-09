"""
Transform and load with SQlite3 database
"""

import sqlite3
import csv
import pandas as pd

csv.field_size_limit(100000000)

# Transform functions
# might need to create two different transforms
# voter reg encoding is Windows-1252
# vote history encoding is ascii

# Transforming voter reg data from encoding="Windows-1252" to encoding="utf-8"


def transform_voterreg(txtfile):
    df = pd.read_csv(
        txtfile, delimiter="\t", encoding="windows-1252", on_bad_lines="skip"
    )

    # Save as UTF-16 encoded CSV
    df.to_csv(
        "output.csv",
        sep="\t",
        encoding="utf-16",
        index=False,
        quoting=csv.QUOTE_MINIMAL,
    )


def load_voterreg(dataset, year):
    db_name = "voterreg_"
    payload = csv.reader(
        open(dataset, newline="", encoding="windows-1252"),
        delimiter="\t",
    )

    conn = sqlite3.connect(f"{db_name}{year}.db")
    c = conn.cursor()
    # generate new table for the database
    c.execute(f"DROP TABLE IF EXISTS {db_name}{year}")
    c.execute(
        f"""
            CREATE TABLE {db_name}{year} (
            county_id INTEGER,
            county_desc TEXT,
            voter_reg_num TEXT,
            ncid TEXT,
            last_name TEXT,
            first_name TEXT,
            middle_name TEXT,
            name_suffix_lbl TEXT,
            status_cd TEXT,
            voter_status_desc TEXT,
            reason_cd TEXT,
            voter_status_reason_desc TEXT,
            res_street_address TEXT,
            res_city_desc TEXT,
            state_cd TEXT,
            zip_code TEXT,
            mail_addr1 TEXT,
            mail_addr2 TEXT,
            mail_addr3 TEXT,
            mail_addr4 TEXT,
            mail_city TEXT,
            mail_state TEXT,
            mail_zipcode TEXT,
            full_phone_number TEXT,
            confidential_ind TEXT,
            regisr_dt TEXT,
            race_code TEXT,
            ethnic_code TEXT,
            party_cd TEXT,
            gender_code TEXT,
            birth_year INTEGER,
            age_at_year_end TEXT,
            birth_state TEXT,
            drivers_lic TEXT,
            precinct_abbrv TEXT,
            precinct_desc TEXT,
            municipality_abbrv TEXT,
            municipality_desc TEXT,
            ward_abbrv TEXT,
            ward_desc TEXT,
            cong_dist_abbrv TEXT,
            super_court_abbrv TEXT,
            judic_dist_abbrv TEXT,
            nc_senate_abbrv TEXT,
            nc_house_abbrv TEXT,
            county_commiss_abbrv TEXT,
            county_commiss_desc TEXT,
            township_abbrv TEXT,
            township_desc TEXT,
            school_dist_abbrv TEXT,
            school_dist_desc TEXT,
            fire_dist_abbrv TEXT,
            fire_dist_desc TEXT,
            water_dist_abbrv TEXT,
            water_dist_desc TEXT,
            sewer_dist_abbrv TEXT,
            sewer_dist_desc TEXT,
            sanit_dist_abbrv TEXT,
            sanit_dist_desc TEXT,
            rescue_dist_abbrv TEXT,
            rescue_dist_desc TEXT,
            munic_dist_abbrv TEXT,
            munic_dist_desc TEXT,
            dist_1_abbrv TEXT,
            dist_1_desc TEXT,
            vtd_abbrv TEXT,
            vtd_description TEXT)
            """
    )
    # insert values
    c.executemany(
        f"""
            INSERT INTO {db_name}{year} (
            county_id,
            county_desc,
            voter_reg_num,
            ncid,
            last_name,
            first_name,
            middle_name,
            name_suffix_lbl,
            status_cd,
            voter_status_desc,
            reason_cd,
            voter_status_reason_desc,
            res_street_address,
            res_city_desc,
            state_cd,
            zip_code,
            mail_addr1,
            mail_addr2,
            mail_addr3,
            mail_addr4,
            mail_city,
            mail_state,
            mail_zipcode,
            full_phone_number,
            confidential_ind,
            regisr_dt,
            race_code,
            ethnic_code,
            party_cd,
            gender_code,
            birth_year,
            age_at_year_end,
            birth_state,
            drivers_lic,
            precinct_abbrv,
            precinct_desc,
            municipality_abbrv,
            municipality_desc,
            ward_abbrv,
            ward_desc,
            cong_dist_abbrv,
            super_court_abbrv,
            judic_dist_abbrv,
            nc_senate_abbrv,
            nc_house_abbrv,
            county_commiss_abbrv,
            county_commiss_desc,
            township_abbrv,
            township_desc,
            school_dist_abbrv,
            school_dist_desc,
            fire_dist_abbrv,
            fire_dist_desc,
            water_dist_abbrv,
            water_dist_desc,
            sewer_dist_abbrv,
            sewer_dist_desc,
            sanit_dist_abbrv,
            sanit_dist_desc,
            rescue_dist_abbrv,
            rescue_dist_desc,
            munic_dist_abbrv,
            munic_dist_desc,
            dist_1_abbrv,
            dist_1_desc,
            vtd_abbrv,
            vtd_description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()
    conn.close()
    return f"{db_name}{year}.db"


### not working right now, save for code reference
# def load_voterreg(dataset, year):
#     db_name = "voterreg_"

#     # Read the CSV file and prepare the payload
#     payload = []
#     with open(dataset, newline="", encoding="utf-16-le") as csvfile:
#         reader = csv.reader(csvfile, delimiter="\t")
#         for row in reader:
#             if len(row) == 67:  # Ensure there are exactly 67 columns
#                 payload.append(tuple(row))  # Append as a tuple
#             else:
#                 print(f"Warning: Row does not contain 67 columns: {row}")

#     conn = sqlite3.connect(f"{db_name}{year}.db")
#     c = conn.cursor()

#     # Create table
#     c.execute(f"DROP TABLE IF EXISTS {db_name}{year}")
#     c.execute(
#         f"""
#         CREATE TABLE {db_name}{year} (
#             county_id INTEGER,
#             county_desc TEXT,
#             voter_reg_num TEXT,
#             ncid TEXT,
#             last_name TEXT,
#             first_name TEXT,
#             middle_name TEXT,
#             name_suffix_lbl TEXT,
#             status_cd TEXT,
#             voter_status_desc TEXT,
#             reason_cd TEXT,
#             voter_status_reason_desc TEXT,
#             res_street_address TEXT,
#             res_city_desc TEXT,
#             state_cd TEXT,
#             zip_code TEXT,
#             mail_addr1 TEXT,
#             mail_addr2 TEXT,
#             mail_addr3 TEXT,
#             mail_addr4 TEXT,
#             mail_city TEXT,
#             mail_state TEXT,
#             mail_zipcode TEXT,
#             full_phone_number TEXT,
#             confidential_ind TEXT,
#             regisr_dt TEXT,
#             race_code TEXT,
#             ethnic_code TEXT,
#             party_cd TEXT,
#             gender_code TEXT,
#             birth_year INTEGER,
#             age_at_year_end TEXT,
#             birth_state TEXT,
#             drivers_lic TEXT,
#             precinct_abbrv TEXT,
#             precinct_desc TEXT,
#             municipality_abbrv TEXT,
#             municipality_desc TEXT,
#             ward_abbrv TEXT,
#             ward_desc TEXT,
#             cong_dist_abbrv TEXT,
#             super_court_abbrv TEXT,
#             judic_dist_abbrv TEXT,
#             nc_senate_abbrv TEXT,
#             nc_house_abbrv TEXT,
#             county_commiss_abbrv TEXT,
#             county_commiss_desc TEXT,
#             township_abbrv TEXT,
#             township_desc TEXT,
#             school_dist_abbrv TEXT,
#             school_dist_desc TEXT,
#             fire_dist_abbrv TEXT,
#             fire_dist_desc TEXT,
#             water_dist_abbrv TEXT,
#             water_dist_desc TEXT,
#             sewer_dist_abbrv TEXT,
#             sewer_dist_desc TEXT,
#             sanit_dist_abbrv TEXT,
#             sanit_dist_desc TEXT,
#             rescue_dist_abbrv TEXT,
#             rescue_dist_desc TEXT,
#             munic_dist_abbrv TEXT,
#             muni_dist_desc TEXT,
#             dist_1_abbrv TEXT,
#             dist_1_desc TEXT,
#             vtd_abbrv TEXT,
#             vtd_desc TEXT
#         )
#         """
#     )

#     # Insert values
#     c.executemany(
#         f"""
#         INSERT INTO {db_name}{year} (
#             county_id,
#             county_desc,
#             voter_reg_num,
#             ncid,
#             last_name,
#             first_name,
#             middle_name,
#             name_suffix_lbl,
#             status_cd,
#             voter_status_desc,
#             reason_cd,
#             voter_status_reason_desc,
#             res_street_address,
#             res_city_desc,
#             state_cd,
#             zip_code,
#             mail_addr1,
#             mail_addr2,
#             mail_addr3,
#             mail_addr4,
#             mail_city,
#             mail_state,
#             mail_zipcode,
#             full_phone_number,
#             confidential_ind,
#             regisr_dt,
#             race_code,
#             ethnic_code,
#             party_cd,
#             gender_code,
#             birth_year,
#             age_at_year_end,
#             birth_state,
#             drivers_lic,
#             precinct_abbrv,
#             precinct_desc,
#             municipality_abbrv,
#             municipality_desc,
#             ward_abbrv,
#             ward_desc,
#             cong_dist_abbrv,
#             super_court_abbrv,
#             judic_dist_abbrv,
#             nc_senate_abbrv,
#             nc_house_abbrv,
#             county_commiss_abbrv,
#             county_commiss_desc,
#             township_abbrv,
#             township_desc,
#             school_dist_abbrv,
#             school_dist_desc,
#             fire_dist_abbrv,
#             fire_dist_desc,
#             water_dist_abbrv,
#             water_dist_desc,
#             sewer_dist_abbrv,
#             sewer_dist_desc,
#             sanit_dist_abbrv,
#             sanit_dist_desc,
#             rescue_dist_abbrv,
#             rescue_dist_desc,
#             munic_dist_abbrv,
#             muni_dist_desc,
#             dist_1_abbrv,
#             dist_1_desc,
#             vtd_abbrv,
#             vtd_desc
#         )
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """,
#         payload,
#     )

#     conn.commit()
#     conn.close()
#     return f"{db_name}{year}.db"


# def load_votehistory(dataset):


def load_pollingplaces(dataset, year):
    data = open(dataset, newline="", encoding="utf-16")
    # NCSBE data includes null bytes, which must be removed
    payload = csv.reader((line.replace("\0", "") for line in data), delimiter="\t")
    db_name = "pollingplaces_"
    conn = sqlite3.connect(f"{db_name}{year}.db")
    c = conn.cursor()
    # generate new table for the database
    c.execute(f"DROP TABLE IF EXISTS {db_name}{year}")
    c.execute(
        f"""
            CREATE TABLE {db_name}{year} (
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
        f"""
            INSERT INTO {db_name}{year} (
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
    return f"{db_name}{year}.db"


if __name__ == "__main__":
    load_pollingplaces(dataset="./data/pollingplaces_2020.csv", year=2020)
    load_voterreg(dataset="./data/ncvoter32.txt", year=2020)
