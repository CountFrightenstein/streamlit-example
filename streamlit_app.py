import streamlit as st
import pandas as pd
import numpy as np
from joblib import load
import time
import os
import csv
import re

# Example usage
# Names of the models used in the ensemble
model_names = ['Linear_Regression', 'Ridge', 'Lasso', 'ElasticNet', 'SVR', 'Decision_Tree', 
               'Random_Forest', 'Gradient_Boosting', 'XGBoost', 'CatBoost', 
               'MLP_Neural_Network']

def export_dataframe_to_excel(df):
    df['buy_proportion'] = df['buy_proportion'].str.rstrip('%').astype(float) / 100.0
    print_value = df.iloc[0]['print']
    actual_drop_date = df.iloc[0]['actual_drop_date'].strftime('%Y-%m-%d')
    # Ask the user for the location and name of the Excel file to save
    unix_timestamp = int(time.time())
    default_filename = f"{print_value}_{str(actual_drop_date)}_{str(unix_timestamp)}.xlsx"

    # Ask the user for the location and name of the Excel file to save, suggesting the default filename
    file_path = st.file_uploader("Save As", type=["xlsx"], accept_multiple_files=False, key="file_uploader")
    if file_path:  # Check if a file path was selected
        try:
            df.to_excel(file_path, index=False)
            st.success("The data was exported successfully to Excel.")
        except Exception as e:
            st.error(f"Failed to export the data: {e}")

def display_dataframe(df):
    # Display the DataFrame
    st.dataframe(df)

    # Button for exporting DataFrame to Excel
    if st.button("Export to Excel"):
        export_dataframe_to_excel(df)

# Load models and predict
def load_and_predict(models, X):
    predictions = []
    for name in models:
        model = load(f'model/{name}.joblib')
        preds = model.predict(X)
        predictions.append(preds)
    # Average predictions to get the ensemble prediction
    ensemble_preds = np.mean(predictions, axis=0)
    return ensemble_preds

def data_processor(df):
    features = ['product_type', 'variant', #'product_price', 
            'actual_drop_date',
            'drop_month', 'print', 'season', 'drop_time_holiday', 'print_gender',
            #'product_gender', 
            'main_color', 'designcat', 'designelement',
            'returning_cohort', 'first_time_cohort', 'returning_sales',
            'first_time_sales', 'order_quantity', 
            'confidence_score','convertible_romper_in_drop',
            'pajama_set_in_drop', 'romper_in_drop', 'footie_in_drop', 'prints_in_drop']
    
    # Splitting dataset into features and target variable
    X = df[features]
    preprocessor = load('preprocessor/preprocessor.joblib')    
    #columns = preprocessor.get_feature_names_out()
    #X_new_transformed_df = pd.DataFrame(X_new_transformed, columns=columns)

    X = preprocessor.transform(X)
    return X

def on_color_focus_out(event):
    combobox = event.widget
    user_input = combobox.get().strip()

    # Check if the user input is the placeholder text and ignore it
    if user_input.lower() == 'select or type':
        combobox.set('')
        return

    normalized_user_input = normalize_string(user_input)
    normalized_options = [normalize_string(option) for option in unique_main_colors]

    if normalized_user_input in normalized_options:
        actual_option = unique_main_colors[normalized_options.index(normalized_user_input)]
        combobox.set(actual_option)
    elif user_input:
        # Ask if the user wants to add the new item
        if st.button(f"Add New Color: {user_input}"):
            add_new_color(user_input)
        else:
            combobox.set('')

def on_designcat_focus_out(event):
    combobox = event.widget
    user_input = combobox.get().strip()

    # Check if the user input is the placeholder text and ignore it
    if user_input.lower() == 'select or type':
        combobox.set('')
        return

    normalized_user_input = normalize_string(user_input)
    normalized_options = [normalize_string(option) for option in unique_design_cats]

    if normalized_user_input in normalized_options:
        actual_option = unique_design_cats[normalized_options.index(normalized_user_input)]
        combobox.set(actual_option)
    elif user_input:
        # Ask if the user wants to add the new item
        if st.button(f"Add New Design Category: {user_input}"):
            add_new_designcat(user_input)
        else:
            combobox.set('')

def on_designelement_focus_out(event):
    combobox = event.widget
    user_input = combobox.get().strip()

    # Check if the user input is the placeholder text and ignore it
    if user_input.lower() == 'select or type':
        combobox.set('')
        return

    normalized_user_input = normalize_string(user_input)
    normalized_options = [normalize_string(option) for option in unique_design_elements]

    if normalized_user_input in normalized_options:
        actual_option = unique_design_elements[normalized_options.index(normalized_user_input)]
        combobox.set(actual_option)
    elif user_input:
        # Ask if the user wants to add the new item
        if st.button(f"Add New Design Element: {user_input}"):
            add_new_element(user_input)
        else:
            combobox.set('')

def on_print_focus_out(event):
    combobox = event.widget
    user_input = combobox.get().strip()

    # Check if the user input is the placeholder text and ignore it
    if user_input.lower() == 'select or type':
        combobox.set('')
        return

    normalized_user_input = normalize_string(user_input)
    normalized_options = [normalize_string(option) for option in unique_prints]

    if normalized_user_input in normalized_options:
        actual_option = unique_prints[normalized_options.index(normalized_user_input)]
        combobox.set(actual_option)
    elif user_input:
        # Ask if the user wants to add the new item
        if st.button(f"Add New Print: {user_input}"):
            add_new_print(user_input)
        else:
            combobox.set('')

def on_drop_time_holiday_focus_out(event):
    combobox = event.widget
    user_input = combobox.get().strip()

    # Check if the user input is the placeholder text and ignore it
    if user_input.lower() == 'select or type':
        combobox.set('')
        return

    normalized_user_input = normalize_string(user_input)
    normalized_options = [normalize_string(option) for option in unique_drop_time_holidays]

    if normalized_user_input in normalized_options:
        actual_option = unique_drop_time_holidays[normalized_options.index(normalized_user_input)]
        combobox.set(actual_option)
    elif user_input:
        # Ask if the user wants to add the new holiday
        if st.button(f"Add New Drop Time Holiday: {user_input}"):
            add_new_drop_time_holiday(user_input)
        else:
            combobox.set('')

def add_new_color(new_color):
    unique_main_colors.append(new_color)
    
    with open(main_colors_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([new_color])
def add_new_designcat(new_cat):
    unique_design_cats.append(new_cat)
    
    with open(design_cats_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([new_cat])
def add_new_element(new_element):
    unique_design_elements.append(new_element)
    
    with open(design_elements_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([new_element])

def add_new_print(new_print):
    unique_prints.append(new_print)
    
    with open(prints_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([new_print])

def add_new_drop_time_holiday(new_drop_time_holiday):
    unique_drop_time_holidays.append(new_drop_time_holiday)
    
    with open(drop_time_holidays_file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([new_drop_time_holiday])

def get_unique_values(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        values = set(row[0] for row in reader)
        return sorted(list(values))

def normalize_string(s):
    return re.sub(r'\W+', '', s.lower())

def validate_order_quantity(input):
    if input.isdigit():
        return True
    else:
        return False

st.title("Bums & Roses Buy Model")

prints_file_path = 'prints.csv'
unique_prints = get_unique_values(prints_file_path)

main_colors_file_path = 'colors.csv'
unique_main_colors = get_unique_values(main_colors_file_path)

design_cats_file_path = 'designcats.csv'
unique_design_cats = get_unique_values(design_cats_file_path)

design_elements_file_path = 'designelements.csv'
unique_design_elements = get_unique_values(design_elements_file_path)

seasons_file_path = 'seasons.csv'
unique_seasons = get_unique_values(seasons_file_path)

drop_time_holidays_file_path = 'holidays.csv'
unique_drop_time_holidays = get_unique_values(drop_time_holidays_file_path)

confidence_scores = ['A', 'B', 'C']
yes_no_options = ['Yes', 'No']
gender_options = ['Boy', 'Girl', 'Neutral']

entries = []
columns = ['actual_drop_date', 'print', 'season', 'drop_time_holiday', 'print_gender', 'main_color', 'designcat', 'designelement', 'order_quantity', 'confidence_score', 'convertible_romper_in_drop', 'pajama_set_in_drop', 'romper_in_drop','footie_in_drop', 'prints_in_drop']
column_titles = ['Drop Date', 'Print', 'Season', 'Holiday', 'Gender', 'Main Color', 'Design Category', 'Design Element', 'Total Estimated Buy', 'Confidence', 'Convertible Romper in Drop?', 'Pajama Set in Drop?', 'Romper in Drop', 'Footie in Drop', 'Total Prints in Buy']

for column, title in zip(columns, column_titles):
    if column == 'actual_drop_date':
        entry = st.date_input(title, key=column)
    elif column == 'print':
        entry = st.selectbox(title, unique_prints, key=column)
    elif column == 'drop_time_holiday':
        entry = st.selectbox(title, unique_drop_time_holidays, key=column)
    elif column == 'main_color':
        entry = st.selectbox(title, unique_main_colors, key=column)
    elif column == 'designcat':
        entry = st.selectbox(title, unique_design_cats, key=column)
    elif column == 'designelement':
        entry = st.selectbox(title, unique_design_elements, key=column)
    elif column == 'season':
        entry = st.selectbox(title, unique_seasons, key=column)
    elif column == 'order_quantity':
        entry = st.number_input(title, key=column)
    elif column == 'confidence_score':
        entry = st.selectbox(title, confidence_scores, key=column)
    elif column == 'convertible_romper_in_drop' or column == 'pajama_set_in_drop' or column == 'romper_in_drop' or column == 'footie_in_drop':
        entry = st.radio(title, options=yes_no_options, key=column)
    elif column == 'prints_in_drop':
        entry = st.number_input(title, key=column)
    elif column == 'print_gender':
        entry = st.selectbox(title, gender_options, key=column)
    else:
        entry = st.text_input(title, key=column)

    entries.append((column, entry))

if st.button("Run Model"):
    # Processing the data
    df = pd.DataFrame(columns=[col for col, _ in entries])
    for col, entry in entries:
        if isinstance(entry, pd.Timestamp):
            df[col] = [entry]
        elif isinstance(entry, (int, float)):
            df[col] = [entry]
        elif isinstance(entry, str):
            df[col] = [entry]

    X = data_processor(df)
    results = load_and_predict(model_names, X)

    df['sales_projection'] = results
    all_sales = df['sales_projection'].sum()
    df['buy_proportion'] = df['sales_projection'] / all_sales
    df['implied_buy'] = df['buy_proportion'] * df['total_buy']
    df['sales_projection'] = df['sales_projection'].round().astype(int)
    df['implied_buy'] = df['implied_buy'].round().astype(int)

    # Convert buy_proportion to percentage format and round to 2 decimal places
    df['buy_proportion'] = (df['buy_proportion'] * 100).round(2).astype(str) + '%'
    df = df.sort_values(by=['product_type', 'size_sort'])[['actual_drop_date', 'print', 'season', 'drop_time_holiday', 'print_gender', 'main_color', 'designcat', 'designelement', 'product_type', 'variant', 'returning_cohort', 'first_time_cohort', 'confidence_score', 'sales_projection', 'buy_proportion', 'implied_buy']]

    # Display the DataFrame in a new window
    display_dataframe(df)
