"""
Main cli or app entry point
"""

from mylib.extract import extract_zip
from mylib.transform_load import (
    transform_voterreg,
    transform_votehistory,
    load_voterreg,
    load_votehistory,
)
from mylib.query import general_query
import os
from datetime import datetime

# Extract, Transform and Load
# Query


def main_results():
    extract_zip(
        url="https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip",
        directory="data",
    )
    extract_zip(
        url="https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis32.zip", directory="data"
    )
    today = datetime.now()
    main_directory = "/Users/pdeguz01/Documents/git/PeterdeGuzman_Mini6/"
    os.chdir(main_directory)
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
    general_query(
        """ SELECT COUNT(*) AS count_of_race_b FROM ped19voterreg_2024_10_14 WHERE race = 'b' """
    )
    # general_query()


main_results()
