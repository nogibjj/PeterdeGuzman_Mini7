"""
Extracting dataset from CSV files hosted online
"""

import requests
import os
import zipfile
import chardet
import io


# test encoding function of zipped files accessible by a URL
def test_encoding_zippedfile(url):
    response = requests.get(url)
    response.raise_for_status()  # Check for any errors during download

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # List files in the zip archive
        print("Files in the zip archive:")
        print(z.namelist())

        # Read the .txt file (assuming the file name is known)
        # If you want to check all text files, you may want to loop through z.namelist()
        for filename in z.namelist():
            if filename.endswith(".txt"):
                with z.open(filename) as txt_file:
                    content = txt_file.read()
                    encoding_info = chardet.detect(content)
                    print(f"\nEncoding of {filename}: {encoding_info['encoding']}")


def check_txtencoding_directory(directory):
    # Loop through all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            file_path = os.path.join(directory, filename)

            # Open the file in binary mode
            with open(file_path, "rb") as f:
                raw_data = f.read()

                # Detect encoding
                result = chardet.detect(raw_data)
                encoding = result["encoding"]
                confidence = result["confidence"]

                print(f"File: {filename}")
                print(f"Detected encoding: {encoding} (Confidence: {confidence:.2f})")


def extract(
    url,
    filepath,
    directory,
):
    """Extract to file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url, timeout=5) as r:
        with open(filepath, "wb") as f:
            f.write(r.content)
    return filepath


# add filename argument so you can custom name the downloaded file


def extract_zip(url, directory):
    # Creating directory if not present
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Creating filepath for zipped file
    zip_filepath = os.path.join(directory, "downloaded_file.zip")
    # Downloading zipped file
    with requests.get(url, timeout=5) as r:
        r.raise_for_status()  # Raise an error for bad responses
        with open(zip_filepath, "wb") as f:
            f.write(r.content)
    # Extracting zip file
    with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
        zip_ref.extractall(directory)
    # removing zipped file after extraction
    os.remove(zip_filepath)
    return directory


# if __name__ == "__main__":
#     if os.path.exists("/Users/pdeguz01/Documents/git/PeterdeGuzman_Mini6"):
#         os.chdir("/Users/pdeguz01/Documents/git/PeterdeGuzman_Mini6")
#     else:
#         print("Directory does not exist.")
# extract(
#     url="https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2020_11_03/polling_place_20201103.csv",
#     filepath="data/pollingplaces_2020.csv",
#     directory="data",
# )
# extract_zip(
#     url="https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip",
#     directory="data",
# )
# extract_zip(
#     url="https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis32.zip", directory="data"
# )
