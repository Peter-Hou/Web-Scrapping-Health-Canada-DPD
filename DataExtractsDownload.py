import streamlit as st
import requests
import io
import zipfile
import base64

def download_zip_files(download = True):
    file_urls = {
        'allfiles.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles.zip",
        'allfiles_ia.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_ia.zip",
        'allfiles_ap.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_ap.zip",
        'allfiles_dr.zip': "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles_dr.zip"
    }

    def filedownload(zip_file, filename):
        b64 = base64.b64encode(zip_file).decode()  # strings <-> bytes conversions
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}"> Click to Download {filename} </a>'
        return href

    if download:
        with st.spinner('Downloading...'):
            for file_name, url in file_urls.items():
                response = requests.get(url, stream=True).content
                # Write the file to the local file system
                st.markdown(filedownload(response, file_name), unsafe_allow_html=True)

    else:
        with st.spinner('Getting Data Extracts Files Ready...'):
            zip_files = {}
            for file_name, url in file_urls.items():
                # Send a request to download the file content
                response = requests.get(url)

                # Read the file content as a BytesIO object
                zip_file = io.BytesIO(response.content)

                zip_files[file_name] = zip_file
            return zip_files


