import streamlit as st
import requests
import io
import zipfile

def download_zip_files(download = True):
    file_urls = {
        'allfiles.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles.zip",
        'allfiles_ia.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_ia.zip",
        'allfiles_ap.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_ap.zip",
        'allfiles_dr.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_dr.zip"
    }

    if download:
        with st.spinner('Downloading...'):
            for file_name, url in file_urls.items():
                req = requests.get(url, stream=True)
                # Write the file to the local file system
                with open(file_name, 'wb') as output_file:
                    for chunk in req.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output_file.write(chunk)

                st.write(f'{file_name} Downloading Completed')
    else:
        zip_files = {}
        for file_name, url in file_urls.items():
            # Send a request to download the file content
            response = requests.get(url)

            # Read the file content as a BytesIO object
            zip_file = io.BytesIO(response.content)

            zip_files[file_name] = zip_file
        return zip_files


