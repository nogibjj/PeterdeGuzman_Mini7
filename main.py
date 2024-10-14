"""
Main cli or app entry point
"""

from mylib.extract import extract_zip
from mylib.transform_load import (
    transform_voterreg,
    transform_votehistory,
    load_voterreg,
    trim_dataset,
    load_votehistory,
)
from mylib.query import general_query
import os
from test_main import test_extract_zip, test_load_voterreg, test_load_votehistory


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
    trim_dataset(
        dataset=f"{main_directory}/data/voterreg_Durham241011.csv",
        dataset_type="voterreg",
        n=5000,
        directory="data",
    )
    trim_dataset(
        dataset=f"{main_directory}/data/votehist_Durham241011.csv",
        dataset_type="voterhist",
        n=5000,
        directory="data",
    )
    load_voterreg(dataset=f"{main_directory}data/trimmed_voterreg.csv")
    load_votehistory(dataset=f"{main_directory}/data/trimmed_voterhist.csv")
    general_query(
        """ SELECT voted_party_desc, COUNT(*) AS total_count 
        FROM ped19_voterreg AS t1 JOIN ped19_voterhist AS t2 ON t1.ncid = t2.ncid 
        GROUP BY voted_party_desc ORDER BY total_count DESC """
    )
    test_extract_zip()
    test_load_voterreg()
    test_load_votehistory()


main_results()
