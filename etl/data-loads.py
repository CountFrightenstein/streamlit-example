#CONNECT TO DRIVE TO EXTRACT DATA FILES

from google.oauth2 import service_account
from googleapiclient.discovery import build
import io
from googleapiclient.http import MediaIoBaseDownload
import os
from googleapiclient.http import MediaFileUpload

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'credentials.json'

# Function to authenticate and create the service using service account
def service_creation():
    credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials)

def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")
# Function to list files in a folder
def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents"
    results = service.files().list(q=query,
                                   spaces='drive',
                                   fields='nextPageToken, files(id, name)',
                                   pageToken=None).execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))



# Function to upload a file
def upload_file(service, file_name, file_path, mime_type, folder_id=None):
    file_metadata = {'name': file_name}
    if folder_id:  # If providing a folder ID, set this file to be created in that folder
        file_metadata['parents'] = [folder_id]
    media = MediaFileUpload(file_path,
                            mimetype=mime_type)
    file = service.files().create(body=file_metadata,
                                  media_body=media,
                                  fields='id').execute()
    print(f'File ID: {file.get("id")} uploaded.')


#LIST FOLDERS
service = service_creation()
folder_id = '1MBsER9h47Zi6aMbVU3myeCjXCIkgbdDF'  # Replace YOUR_FOLDER_ID_HERE with the actual folder ID
list_files_in_folder(service, folder_id)

#DOWNLOAD FILES
download_file(service, '1LIvRDJzn3j53nIwnE-4LdRy35YzuHCC2', 'data-pull/master-legend.xlsx')
download_file(service, '1JKS1bvrSyTXBQGCr9vZm2drsOGprm2qy', 'data-pull/size-map.xlsx')
download_file(service, '1g1Dj9aLkbVBhBei3s-oSOzm0V1Vr8ZNg', 'data-pull/order-log.xlsx')
download_file(service, '1s0GuK_V32AS-pPYJjww57IzB4B5hJBxO', 'data-pull/sales.csv')
download_file(service, '1t-M8CPfbO4saXETVM9GvtdNpIHOjQCNU', 'data-pull/sales24.csv')
download_file(service, '1JSTy-htuxAOBRtZYZrVYlZq28tn4IaBm', 'data-pull/sales23.csv')
download_file(service, '1ZBV1ubRlRpJmt0Is8rfTmaWUrrQ-IKJf', 'data-pull/sales22.csv')
download_file(service, '1aO7xP14hpvpGyOBtZL6Dwl0fSd6LhmkA', 'data-pull/sales21.csv')
download_file(service, '1AfP1eXaOZP0aS0UurF2aRgy6qzeDyNcs', 'data-pull/model_input.csv')
download_file(service, '16Oo6nLdzsNDEdMeQbu2s2g99sfPzglga', 'data-pull/ad-spend-by-day.csv')
download_file(service, '192hrjlXaxK8tG5WGlp1uI0RwSrHzMiTZ', 'data-pull/confidence-scores.csv')

#REMOVE $ FROM MARKETING SPEND FILE
import pandas as pd
pd.set_option('display.max_columns', None)
df = pd.read_csv('data-pull/ad-spend-by-day.csv', delimiter='\t',)
df['spend'] = df['Spend'].replace('[\$,]', '', regex=True).astype(float)
df.to_csv('data-pull/ad-spend-by-day.csv', index=False)

#SETTING FILE PATHS
base_path = 'data-pull'
file_path_2021 = f"{base_path}/sales21.csv"
file_path_2022 = f"{base_path}/sales22.csv"
file_path_2023 = f"{base_path}/sales23.csv"
file_path_2024 = f"{base_path}/sales24.csv"
file_sales_2023 = f"{base_path}/sales.csv"
file_legend_2023 = f"{base_path}/master-legend.xlsx"
file_orderlog_2023 = f"{base_path}/order-log.xlsx"
file_market_spend = f"{base_path}/ad-spend-by-day.csv"
file_confidence_score = f"{base_path}/confidence-scores.csv"
file_size_map = f"{base_path}/size-map.xlsx"

#PROCESS NECESSARY TABS FROM EXCEL INPUTS
legend_sheets = ['Legend', 'Regular or Holiday', 'Print Gender', 'Product Type Genders', 'Print Color + Category + Design', 'Unique Products']
for sheet in legend_sheets:
    pd.read_excel(file_legend_2023, sheet_name=sheet).to_csv(file_legend_2023.replace('.xlsx', f"_{sheet.lower().replace(' ','_')}.csv"), index=False)
order_log_sheets = ['Order Log', 'Master SKU List']#, 'Confidence Score Historical (WIP)']
for sheet in order_log_sheets:
    pd.read_excel(file_orderlog_2023, sheet_name=sheet).to_csv(file_orderlog_2023.replace('.xlsx', f"_{sheet.lower().replace(' ','_')}.csv"), index=False)
size_map_sheets = ['Hoja1']
for sheet in size_map_sheets:
    pd.read_excel(file_size_map, sheet_name=sheet).to_csv(file_size_map.replace('.xlsx', f"_{sheet.lower().replace(' ','_')}.csv"), index=False)

#PROCESS THE SALES FILE - NEEDED ONLY FOR SEASON AT THIS POINT

df = pd.read_csv(file_sales_2023)
df['Marketing Spend Particular Print ($)'] = df['Marketing Spend Particular Print ($)'].replace('[\$,]', '', regex=True).astype(float)

df=df[df['SKU']!='123456789']
df = df.dropna(axis=1, how='all')
column_mapping = {
    'SKU': 'SKU',
    'Product Title': 'Product_Title',
    'Drop dates (MM-DD-YYYY)': 'Drop_Dates',
    'Print': 'Print',
    'Type': 'Type',
    'Product_type': 'Product_Type',
    'Variant_title': 'Variant_Title',
    'Drop Time Holiday': 'Drop_Time_Holiday',
    'Season': 'Season',
    'Gender': 'Gender',
    'Confidence Score\nAt The Time': 'Confidence_Score',
    'Color': 'Color',
    'Design Elements and Appeal': 'Design_Elements_and_Appeal',
    'Marketing Spend Particular Print ($)': 'Marketing_Spend',
    'Confidence Level in Marketing Strategy': 'Confidence_Level_in_Marketing_Strategy',
    'Days Campaign Duration': 'Days_Campaign_Duration'
}

# Rename the columns
df.rename(columns=column_mapping, inplace=True)
df.to_csv(f"{base_path}/cleaned.csv", index=False)

#CREATING THE DATA TABLES
import duckdb

# Connect to DuckDB. If 'my_duckdb.duckdb' does not exist, it will be created.
con = duckdb.connect('package/database/bums_and_roses.duckdb')


# Load CSV files into DuckDB tables
# Replace 'your_file_path.csv' with the path to your CSV files
con.execute(f"CREATE OR REPLACE TABLE sales_data AS SELECT * FROM read_csv_auto('{file_path_2023}', SAMPLE_SIZE=-1, HEADER=True)")
con.execute(f"INSERT INTO sales_data SELECT * FROM read_csv_auto('{file_path_2022}', SAMPLE_SIZE=-1, HEADER=True)")
con.execute(f"INSERT INTO sales_data SELECT * FROM read_csv_auto('{file_path_2021}', SAMPLE_SIZE=-1, HEADER=True)")
con.execute(f"INSERT INTO sales_data SELECT * FROM read_csv_auto('{file_path_2024}', SAMPLE_SIZE=-1, HEADER=True)")
con.execute(f"CREATE OR REPLACE TABLE sales_data_sku AS SELECT * FROM read_csv_auto('{base_path}/cleaned.csv', HEADER=True)")
#con.execute(f"CREATE OR REPLACE TABLE order_log_data AS SELECT * FROM read_csv_auto('{file_orderlog_2023}', HEADER=True)")
con.execute(f"CREATE OR REPLACE TABLE marketing_spend AS SELECT * FROM read_csv_auto('{file_market_spend}', HEADER=True)")
con.execute(f"CREATE OR REPLACE TABLE confidence_scores AS SELECT * FROM read_csv_auto('{file_confidence_score}', HEADER=True)")
# Example query to check the loaded data
#print(con.execute("SELECT * FROM sales_data LIMIT 5").fetchall())

# Remember to close the connection when done

for sheet in legend_sheets:
    nm = file_legend_2023.replace('.xlsx', f"_{sheet.lower().replace(' ','_')}")
    con.execute(f"CREATE OR REPLACE TABLE {nm.split('/')[1].replace('-', '_').replace('+', '')} AS SELECT * FROM read_csv_auto('{nm}.csv', HEADER=True)")
for sheet in order_log_sheets:
    nm = file_orderlog_2023.replace('.xlsx', f"_{sheet.lower().replace(' ','_')}")
    con.execute(f"CREATE OR REPLACE TABLE {nm.split('/')[1].replace('-', '_').replace('+', '')} AS SELECT * FROM read_csv_auto('{nm}.csv', HEADER=True)")
    print(nm.split('/')[1].replace('-', '_').replace('+', ''))
for sheet in size_map_sheets:
    nm = file_size_map.replace('.xlsx', f"_{sheet.lower().replace(' ','_')}")
    #print(nm)
    con.execute(f"CREATE OR REPLACE TABLE {nm.split('/')[1].replace('-', '_').replace('+', '')} AS SELECT * FROM read_csv_auto('{nm}.csv', HEADER=True)")
con.execute(""" CREATE OR REPLACE TABLE marketing_spend_agg as   
    SELECT Date, Product_Print_Title, sum(spend_1) as spend from marketing_spend group by date, Product_Print_Title """)
con.execute('CREATE OR REPLACE TABLE days (day INTEGER)')

# Insert numbers 1 through 90 into the table
for i in range(1, 91):
    con.execute(f'INSERT INTO days VALUES ({i})')