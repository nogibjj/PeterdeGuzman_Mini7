# import requests
# import zipfile
# import io
# import csv

# # URL of the zipped file
# url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip"

# # Download the zipped file
# response = requests.get(url)
# response.raise_for_status()  # Check for any errors during download

# # Open the zipped file in memory
# with zipfile.ZipFile(io.BytesIO(response.content)) as z:
#     # List files in the zip archive
#     print("Files in the zip archive:")
#     print(z.namelist())

#     # Read the .txt file (replace 'yourfile.txt' with the actual file name)
#     with z.open("ncvoter32.txt") as txt_file:
#         content = txt_file.read().decode("unicode-escape")  # Adjust encoding if needed
#         # Split content into lines (assuming each line is a separate record)
#         lines = content.splitlines()

#         # Write to a tab-delimited CSV file
#         with open("output.csv", "w", newline="", encoding="utf-16") as csv_file:
#             writer = csv.writer(csv_file, delimiter="\t")
#             for line in lines:
#                 # Split each line into columns (assuming tab or space-separated)
#                 writer.writerow(line.split())

#         print("\nContent written to output.csv successfully.")


import requests
import zipfile
import io
import chardet

# URL of the zipped file
# url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip"

# # Download the zipped file
# response = requests.get(url)
# response.raise_for_status()  # Check for any errors during download

# # Open the zipped file in memory
# with zipfile.ZipFile(io.BytesIO(response.content)) as z:
#     # List files in the zip archive
#     print("Files in the zip archive:")
#     print(z.namelist())

#     # Read the .txt file (assuming the file name is known)
#     # If you want to check all text files, you may want to loop through z.namelist()
#     for filename in z.namelist():
#         if filename.endswith(".txt"):
#             with z.open(filename) as txt_file:
#                 content = txt_file.read()
#                 encoding_info = chardet.detect(content)
#                 print(f"\nEncoding of {filename}: {encoding_info['encoding']}")

# Encoding is Windows-1252

# Note: Make sure you have the chardet library installed
# You can install it using: pip install chardet

url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis32.zip"


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
