import requests
import io
import zipfile
import streamlit as st
from DataExtractsDownload import download_zip_files
from bs4 import BeautifulSoup
import pandas as pd
import base64
import zipfile
import numpy as np
import os

def get_processed_dataframes():
    zip_files = download_zip_files(download = False)

    with zipfile.ZipFile(zip_files['allfiles.zip'], 'r') as allfiles_zip:
        af_names = allfiles_zip.namelist()

    with zipfile.ZipFile(zip_files['allfiles_ia.zip'], 'r') as allfiles_ia_zip:
        af_ia_names = allfiles_ia_zip.namelist()

    with zipfile.ZipFile(zip_files['allfiles_ap.zip'], 'r') as allfiles_ap_zip:
        af_ap_names = allfiles_ap_zip.namelist()

    with zipfile.ZipFile(zip_files['allfiles_dr.zip'], 'r') as allfiles_dr_zip:
        af_dr_names = allfiles_dr_zip.namelist()

    all_file_names = af_names.copy()

    all_file_names.extend(af_ia_names)
    all_file_names.extend(af_ap_names)
    all_file_names.extend(af_dr_names)
    with st.spinner('Getting Raw Data Headers'):
        # Get URL for the page containing column names
        url = "https://www.canada.ca/en/health-canada/services/drugs-health-products/drug-products/drug-product-database/read-file-drug-product-database-data-extract.html"

        # Make a request to the webpage and get the HTML content
        html_content = requests.get(url).text

        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all the tables in the webpage
        tables = soup.find_all('table')

        # Initialize an empty dictionary to store the column names
        column_names_dict = {}
        column_names = []

        # Loop through each table in the webpage
        for table in tables:

            # Find the title of the table (i.e., the file name)
            table_title = table.find('strong').text.strip()

            # Find all the rows in the table
            rows = table.find_all('tr')

            # Extract the column names from the first row of the table
            for row in rows:
                td = row.find('td')
                column_names.append(td.text.strip("*")) if td else None

            # Store the column names in the dictionary
            column_names_dict[table_title] = column_names
            column_names = []

        column_names_dict['QRYM_BIOSIMILAR'] = ['DRUG_CODE', 'TYPE', 'TYPE_F', 'CLASS_CODE']

        def remove_filename_suffix(filename):
            if '_' in filename:
                filename = filename[:filename.find('_')]
            else:
                filename = filename[:filename.find('.')]
            return filename

        def remove_formal_filename_prefix(formal_filename):
            start_ind = formal_filename.find("_")
            return formal_filename[start_ind + 1:].lower()

        def create_name_mapping(file_names, formal_file_names):
            name_mapping = {}
            for filename in file_names:
                original_filename = filename
                filename = remove_filename_suffix(filename)
                not_match = True
                while not_match:
                    for formal_filename in formal_file_names:
                        #print(formal_filename)
                        #print(filename)
                        original_formal_filename = formal_filename
                        formal_filename = remove_formal_filename_prefix(formal_filename)
                        ind = formal_filename.find("_") if "_" in formal_filename else len(formal_filename)
                        if filename == formal_filename[:len(filename)] or \
                            filename == formal_filename[ind + 1: ind + len(filename) + 1]:
                            #print(original_filename)
                            #print(original_formal_filename)
                            name_mapping[original_filename] = original_formal_filename
                            not_match = False
                            break
                    if not not_match:
                        break
                    if len(filename) - 1 != 0:
                        filename = filename[:len(filename) - 1]
                    else:
                        raise ValueError(f'Did not find a match column name for {original_filename} when concatenating the column names to data extracts')
            return name_mapping
        # inactive.txt is breaking the consistent order between all the zip files
        # it's information has been included in the other files of allfiles_ia.zip (assumption)
        af_ia_names.remove('inactive.txt')
        name_mapping = create_name_mapping(af_names, column_names_dict.keys())
        name_mapping1 = create_name_mapping(af_ia_names, column_names_dict.keys())
        name_mapping2 = create_name_mapping(af_ap_names, column_names_dict.keys())
        name_mapping3 = create_name_mapping(af_dr_names, column_names_dict.keys())

        name_mapping.update(name_mapping1)
        name_mapping.update(name_mapping2)
        name_mapping.update(name_mapping3)



        for zip_file in zip_files.values():

            with zipfile.ZipFile(zip_file, 'r') as files:
                file_names = files.namelist()

            for file_name in file_names:
                with zipfile.ZipFile(zip_file, 'r') as files:
                    with files.open(file_name) as file:
                        # Find the matched column names
                        if file_name == 'inactive.txt':
                            continue
                        formal_file_name = name_mapping[file_name]
                        column_names = column_names_dict[formal_file_name]

                        df = pd.read_csv(file, sep=',', header=None, names=column_names)
                        globals()[file_name.split('.')[0]] = df

    with st.spinner('Combine Diverse Data Sources'):
        merged_active = drug.merge(biosimilar, on='DRUG_CODE', how='left') \
                          .merge(comp, on='DRUG_CODE', how='left') \
                          .merge(form, on='DRUG_CODE', how='left') \
                          .merge(ingred, on='DRUG_CODE', how='left') \
                          .merge(package, on='DRUG_CODE', how='left') \
                          .merge(pharm, on='DRUG_CODE', how='left') \
                          .merge(route, on='DRUG_CODE', how='left') \
                          .merge(schedule, on='DRUG_CODE', how='left') \
                          .merge(status, on='DRUG_CODE', how='left') \
                          .merge(ther, on='DRUG_CODE', how='left') \
                          .merge(vet, on='DRUG_CODE', how='left')

        merged_inactive = drug_ia.merge(biosimilar_ia, on='DRUG_CODE', how='left') \
                          .merge(comp_ia, on='DRUG_CODE', how='left') \
                          .merge(form_ia, on='DRUG_CODE', how='left') \
                          .merge(ingred_ia, on='DRUG_CODE', how='left') \
                          .merge(package_ia, on='DRUG_CODE', how='left') \
                          .merge(pharm_ia, on='DRUG_CODE', how='left') \
                          .merge(route_ia, on='DRUG_CODE', how='left') \
                          .merge(schedule_ia, on='DRUG_CODE', how='left') \
                          .merge(status_ia, on='DRUG_CODE', how='left') \
                          .merge(ther_ia, on='DRUG_CODE', how='left') \
                          .merge(vet_ia, on='DRUG_CODE', how='left')

        merged_dormant = drug_dr.merge(biosimilar_dr, on='DRUG_CODE', how='left') \
                          .merge(comp_dr, on='DRUG_CODE', how='left') \
                          .merge(form_dr, on='DRUG_CODE', how='left') \
                          .merge(ingred_dr, on='DRUG_CODE', how='left') \
                          .merge(package_dr, on='DRUG_CODE', how='left') \
                          .merge(pharm_dr, on='DRUG_CODE', how='left') \
                          .merge(route_dr, on='DRUG_CODE', how='left') \
                          .merge(schedule_dr, on='DRUG_CODE', how='left') \
                          .merge(status_dr, on='DRUG_CODE', how='left') \
                          .merge(ther_dr, on='DRUG_CODE', how='left') \
                          .merge(vet_dr, on='DRUG_CODE', how='left')

        merged_approved = drug_ap.merge(biosimilar_ap, on='DRUG_CODE', how='left') \
                          .merge(comp_ap, on='DRUG_CODE', how='left') \
                          .merge(form_ap, on='DRUG_CODE', how='left') \
                          .merge(ingred_ap, on='DRUG_CODE', how='left') \
                          .merge(package_ap, on='DRUG_CODE', how='left') \
                          .merge(pharm_ap, on='DRUG_CODE', how='left') \
                          .merge(route_ap, on='DRUG_CODE', how='left') \
                          .merge(schedule_ap, on='DRUG_CODE', how='left') \
                          .merge(status_ap, on='DRUG_CODE', how='left') \
                          .merge(ther_ap, on='DRUG_CODE', how='left') \
                          .merge(vet_ap, on='DRUG_CODE', how='left')
    with st.spinner('Data Cleaning'):

        # Extracts all ingredient codes that are biosimilar type from all files
        active_biosimilar_ingred_codes = merged_active['ACTIVE_INGREDIENT_CODE'][
            merged_active['TYPE'] == 'Biosimilar'].unique()
        inactive_biosimilar_ingred_codes = merged_inactive['ACTIVE_INGREDIENT_CODE'][
            merged_inactive['TYPE'] == 'Biosimilar'].unique()
        dormant_biosimilar_ingred_codes = merged_dormant['ACTIVE_INGREDIENT_CODE'][
            merged_dormant['TYPE'] == 'Biosimilar'].unique()
        approved_biosimilar_ingred_codes = merged_approved['ACTIVE_INGREDIENT_CODE'][
            merged_approved['TYPE'] == 'Biosimilar'].unique()

        # Put all ingredient codes that are biosimilar type into one array
        biosimilar_ingred_codes = np.concatenate((active_biosimilar_ingred_codes, inactive_biosimilar_ingred_codes,
                                                  dormant_biosimilar_ingred_codes, approved_biosimilar_ingred_codes),
                                                 axis=None)

        # Replace all entries in each file which the ingredient code is in biosimilar ingredient code, the class is Human, and the type is NA with biologic type
        active_mask = (merged_active['CLASS'] == 'Human') & (
            merged_active['ACTIVE_INGREDIENT_CODE'].isin(biosimilar_ingred_codes)) & (merged_active['TYPE'].isna())
        merged_active.loc[active_mask, "TYPE"] = 'Biologic'
        merged_active.loc[active_mask, "TYPE_F"] = 'Biologique'

        inactive_mask = (merged_inactive['CLASS'] == 'Human') & (
            merged_inactive['ACTIVE_INGREDIENT_CODE'].isin(biosimilar_ingred_codes)) & (merged_inactive['TYPE'].isna())
        merged_inactive.loc[inactive_mask, 'TYPE'] = 'Biologic'
        merged_inactive.loc[inactive_mask, "TYPE_F"] = 'Biologique'

        dormant_mask = (merged_dormant['CLASS'] == 'Human') & (
            merged_dormant['ACTIVE_INGREDIENT_CODE'].isin(biosimilar_ingred_codes)) & (merged_dormant['TYPE'].isna())
        merged_dormant.loc[dormant_mask, 'TYPE'] = 'Biologic'
        merged_dormant.loc[dormant_mask, "TYPE_F"] = 'Biologique'

        approved_mask = (merged_approved['CLASS'] == 'Human') & (
            merged_approved['ACTIVE_INGREDIENT_CODE'].isin(biosimilar_ingred_codes)) & (merged_approved['TYPE'].isna())
        merged_approved.loc[approved_mask, 'TYPE'] = 'Biologic'
        merged_approved.loc[approved_mask, "TYPE_F"] = 'Biologique'

        # Clean the dataset that has a column with a footnote in its name
        merged_approved.columns = merged_approved.columns.str.replace('Footnote', '')
        merged_inactive.columns = merged_inactive.columns.str.replace('Footnote', '')
        merged_dormant.columns = merged_dormant.columns.str.replace('Footnote', '')
        merged_active.columns = merged_active.columns.str.replace('Footnote', '')

        merged_approved.columns = merged_approved.columns.str.replace(' ', '')
        merged_inactive.columns = merged_inactive.columns.str.replace(' ', '')
        merged_dormant.columns = merged_dormant.columns.str.replace(' ', '')
        merged_active.columns = merged_active.columns.str.replace(' ', '')
        # Combine all datasets into one giant file
        DIN_MASTER = pd.concat([merged_approved, merged_inactive, merged_dormant, merged_active], ignore_index=True)

        return merged_active, merged_inactive, merged_dormant, merged_approved, DIN_MASTER


def get_csv_files(merged_active, merged_inactive, merged_dormant, merged_approved, DIN_MASTER):
    with st.spinner('Processing Files Downloading'):
        def compress_and_download(df, csv_filename, zip_filename):
            # Convert the DataFrame to a CSV
            csv_data = df.to_csv(index=False, encoding='utf-8-sig')

            # Create an in-memory buffer for writing the zip file
            zip_buffer = io.BytesIO()

            # Create a zip file and add the CSV data to it
            with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr(csv_filename, csv_data.encode('utf-8-sig'))

            # Set cursor position to the beginning of the buffer
            zip_buffer.seek(0)

            # Encode the zip buffer to base64
            b64 = base64.b64encode(zip_buffer.getvalue()).decode()

            # Create a download link for the zip file
            href = f'<a href="data:application/zip;base64,{b64}" download="{zip_filename}">Click to download {zip_filename}</a>'

            return href

        active_dins_download = compress_and_download(merged_active, 'Active DINS.csv', 'Active Dins.zip')
        st.markdown(active_dins_download, unsafe_allow_html=True)

        din_master_download = compress_and_download(merged_inactive, 'Inactive Dins.csv', 'Inactive Dins.zip')
        st.markdown(din_master_download, unsafe_allow_html=True)

        din_master_download = compress_and_download(merged_approved, 'Approved Dins.csv', 'Approved Dins.zip')
        st.markdown(din_master_download, unsafe_allow_html=True)

        din_master_download = compress_and_download(merged_dormant, 'Dormant Dins.csv', 'Dormant Dins.zip')
        st.markdown(din_master_download, unsafe_allow_html=True)

        din_master_download = compress_and_download(DIN_MASTER, 'Din Master.csv', 'Din Master.zip')
        st.markdown(din_master_download, unsafe_allow_html=True)

