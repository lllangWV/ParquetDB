import os
import re
import multiprocessing
from functools import partial
import shutil
import requests
import bz2
from bs4 import BeautifulSoup

from parquetdb import config

# Function to download the file
def download_file(file_url, output_path):
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {output_path}")
    else:
        print(f"Failed to download: {file_url}")
        
        
def download_file_mp_task(file_name, url = "https://alexandria.icams.rub.de/data/pbe/", output_dir='.'):
    file_url = url + file_name
    output_path = os.path.join(output_dir, file_name)
    download_file(file_url, output_path)

# Scrape the page to find all file links that match the pattern
def scrape_files(output_dir='data/external/alexandria/uncompressed', n_cores=1):
    
    os.makedirs(output_dir, exist_ok=True)
    # The URL of the page to scrape
    url = "https://alexandria.icams.rub.de/data/pbe/"

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page: {url}")
        return

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links that match the pattern alexandria_***.json.bz2
    file_links = []
    for link in soup.find_all('a', href=True):
        file_name = link['href']
        if re.match(r'alexandria_.*\.json\.bz2', file_name):
            file_links.append(file_name)

    if not file_links:
        print("No files found matching the pattern.")
        return

    # Download each file
    if n_cores!=1:
        print("Using Multiprocessing")
        with multiprocessing.Pool(processes=n_cores) as pool:
            results=pool.map(partial(download_file_mp_task, url=url, output_dir=output_dir), file_links)
    else:
        print("Not Using Multiprocessing")
        for file_link in file_links:
            download_file(file_link, url, output_dir)
        

def decompress_bz2_file(file_name, source_dir='compressed', dest_dir='uncompressed'):
    if file_name.endswith('.bz2'):
        # Path to the .bz2 file
        bz2_file_path = os.path.join(source_dir, file_name)

        # Decompressed file path (remove the .bz2 extension)
        decompressed_file_name = file_name[:-4]
        decompressed_file_path = os.path.join(dest_dir, decompressed_file_name)

        # Decompress the file
        with bz2.BZ2File(bz2_file_path, 'rb') as file_in:
            with open(decompressed_file_path, 'wb') as file_out:
                file_out.write(file_in.read())
        
        print(f"Decompressed: {bz2_file_path} -> {decompressed_file_path}")
        
def decompress_bz2_files(source_dir, dest_dir, n_cores):
    # Ensure the destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # Loop through all files in the source directory
    filenames=os.listdir(source_dir)
    
    if n_cores!=1:
        with multiprocessing.Pool(n_cores) as pool:
            results = pool.map(partial(decompress_bz2_file, source_dir=source_dir, dest_dir=dest_dir), filenames)
    else:
        for filename in filenames:
            decompress_bz2_file(filename, source_dir, dest_dir)

def download_alexandria_3d_database(output_dir, n_cores=8, from_scratch=False):
    n_cores=8
    # Create a folder to save the downloaded files
    if from_scratch and os.path.exists(output_dir):
        print(f"Removing existing directory: {output_dir}")
        shutil.rmtree(output_dir, ignore_errors=True)
        
    os.makedirs(output_dir, exist_ok=True)
    source_directory = os.path.join(output_dir, 'compressed')
    destination_directory = os.path.join(output_dir, 'uncompressed')
    
    
    if len(os.listdir(destination_directory))>0:
        print("Database downloaded already. Skipping download.")
        return destination_directory
    
    
    scrape_files(output_dir=source_directory, n_cores=n_cores)
    decompress_bz2_files(source_directory, destination_directory,n_cores=n_cores)
    
    return destination_directory