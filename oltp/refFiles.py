import pandas as pd
import requests
import zipfile
import os
from urllib.parse import urljoin

def preProcessRefFiles(filePath, columnsToKeep):
    f=pd.read_csv(filePath)
    keep_col = columnsToKeep
    new_f = f[keep_col]
    new_f.to_csv(f"{filePath}_updated.csv", index=False)

def download_and_unzip(base_url,relativePath, filename="mmt_gtfs.zip"):
    
    project_root = os.getcwd()
    extract_path = os.path.join(project_root, relativePath)
    
    # Construct the full URL
    url = urljoin(base_url, filename)
    
    # Download the file
    print(f"Downloading file from {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        # Save the downloaded file
        with open(filename, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully as {filename}")
        
        # Unzip the file
        print("Extracting files...")
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        print("Files extracted successfully")
        
        # Optionally, remove the zip file after extraction
        os.remove(filename)
        print(f"Removed {filename}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

# Use the function
refFilesURL = "http://transitdata.cityofmadison.com/GTFS/"
relativeExtractPath = "files/refFiles/"
# download_and_unzip(refFilesURL, relativeExtractPath)
preProcessRefFiles("files/refFiles/trips.txt", ['route_id', 'trip_id', 'trip_headsign', 'trip_direction_name'])
