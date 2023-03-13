import streamlit as st
import requests
from bs4 import BeautifulSoup

def get_Search_Criteria():
    response = requests.get('https://health-products.canada.ca/dpd-bdpp/').text
    soup = BeautifulSoup(response, 'html.parser')

    # Find all the labels and select elements
    labels = soup.find_all('label')
    selects = soup.find_all('select')

    # Initialize an empty dictionary to store the results
    results = {}

    labels_pos = 0
    selects_pos = 0
    # Loop through the labels and selects
    while labels_pos != len(labels):
        label = labels[labels_pos]
        select = selects[selects_pos]
        label_text = label.text.strip()

        if label_text == 'Search Canada.ca':
            labels_pos += 1
            continue
        elif label['for'] == select['name']:
            # Get the select options and strip any whitespace
            options = [option.text.strip() for option in select.find_all('option')]
            # Add the label and options to the dictionary
            results[label_text] = options
            labels_pos += 1
            selects_pos += 1
        else:
            results[label_text] = []
            labels_pos += 1
    question_options_dict = results
    return question_options_dict
