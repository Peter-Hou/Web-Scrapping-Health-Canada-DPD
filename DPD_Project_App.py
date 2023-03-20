import streamlit as st
from DataExtractsDownload import download_zip_files
from QueryDPD import get_Search_Criteria
from DataExtractsDataWrangling import get_csv_files
from DataExtractsDataWrangling import get_processed_dataframes

st.title("DPD Project")


# TODO: ON 2023-03-19, I comment out this tab2 as it is still under development and it's not the priority task

#tabs = ["Download Data Extracts", "Query DPD"]
#tab1, tab2 = st.tabs(tabs)
#with tab1:

st.subheader('Download Data Extracts')
st.write('This button will download the most updated data extracts zip files to your local default download folder')
download = st.button("Download Zip Files")
final = st.button('Download Processed Files')
if download:
    download_zip_files()
if final:
    merged_active, merged_inactive, merged_dormant, merged_approved, DIN_MASTER = get_processed_dataframes()
    get_csv_files(merged_active, merged_inactive, merged_dormant, merged_approved, DIN_MASTER)


#with tab2:
#    question_options_dict = get_Search_Criteria()

    # Create a dictionary to store the user's answers
#    user_answers = {}

    # Display the questions and inputs
#    for question, options in question_options_dict.items():
#        if not options:
#            user_input = st.text_input(question)
#            user_answers[question] = user_input
#        else:
#            user_input = st.selectbox(question, options)
#           user_answers[question] = user_input

    #for question, answer in user_answers.items():
       # st.write(f"- {question}: {answer}")

#    if st.button("Submit"):
 #       st.write('please Wait')