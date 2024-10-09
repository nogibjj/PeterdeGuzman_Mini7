import os
import requests
import zipfile


def download_and_extract_zip(url, directory):
    # Create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Define the filepath for the ZIP file
    zip_filepath = os.path.join(directory, "downloaded_file.zip")

    # Download the ZIP file
    with requests.get(url, timeout=5) as r:
        r.raise_for_status()  # Raise an error for bad responses
        with open(zip_filepath, "wb") as f:
            f.write(r.content)

    # Extract the ZIP file
    with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
        zip_ref.extractall(directory)

    # Optionally, remove the ZIP file after extraction
    os.remove(zip_filepath)

    return directory


# Example usage
url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter32.zip"
extract_directory = "ncvoter32"
downloaded_dir = download_and_extract_zip(url, extract_directory)
print(f"Downloaded and extracted files to: {downloaded_dir}")
