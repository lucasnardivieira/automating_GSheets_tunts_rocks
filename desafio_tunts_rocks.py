""" This is a solution to the proposed test
    STEPS

    0. Run the commands to install pandas, gspread and oauth2:
        pip3 install --upgrade google-api-python-client oauth2client
        pip3 install gspread
        pip3 install pandas

    1. Read data in Google sheets
    
    2. Transform the data into a pandas DataFrame(DF)

    3. Check the DF aspects and change the header to adequate

    4. Iterate through the DF rows, inspecting values

    5. Apply main function for each row in the DF

"""
import math
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

def connection():
    ''' This function conects to Google client API to acess the file on Google Sheets. '''
    try:
        print("Establishing connection to the API")
        # define the scope
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        # add credentials to the account
        creds = ServiceAccountCredentials.from_json_keyfile_name('tunts-412817-2cc0808f2d58.json',
                                                                scope)
        # authorize the clientsheet
        client = gspread.authorize(creds)
        # get the instance of the Spreadsheet
        sheet = client.open('Engenharia de Software – Desafio Lucas Nardi')
        # get the first sheet of the Spreadsheet
        sheet_instance = sheet.get_worksheet(0)
        print("Connection successfully")
    except Exception as e:
        print(f"Error: {e}")

    return sheet_instance

def extract_data(sheet_instance):
    ''' This function just extract data from the sheet connection.'''
    try:
        print("Colecting sheet values")
        # get all the records of the data
        records_data = sheet_instance.get_values()
        print("Data sheet collected successfully")
    except Exception as e:
        print(f"Error: {e}")

    return records_data

def transform_data(records_data):
    ''' This function converts the JSON format data into a pandas DataFrame and changes the 
        header to adequate. '''
    try:
        print("Transforming data into a DF")
        # convert the json to dataframe
        df = pd.DataFrame.from_dict(records_data)
        columns = ["Matricula", "Aluno", "Faltas", "P1", "P2", "P3", "Situação",
                "Nota para Aprovação Final"]
        df.columns = columns
        print("Data transformed successfully")
    except Exception as e:
        print(f"Error: {e}")

    return df

def calculate_grades(faltas, p1, p2, p3):
    ''' This function receives the necessary information to identify the student's situation. '''
    try:
        print("Calculating grades and checking status")
        faltas_percent = (faltas / NUM_AULAS) * 100
        media = (p1 + p2 + p3) / 3
        info_dict = {"situacao": "",
                "naf": "0"}
        if faltas_percent > 25:
            info_dict["situacao"] = 'Reprovado por Falta'
            info_dict["naf"] = 0
        else:
            if media >= 70:
                info_dict["situacao"] = 'Aprovado'
                info_dict["naf"] = 0
            if 50 <= media < 70:
                info_dict["situacao"] = 'Exame Final'
                info_dict["naf"] = math.ceil(100 - media)
            if media < 50:
                info_dict["situacao"] = 'Reprovado por nota'
                info_dict["naf"] = 0
        print("Status checked")
    except Exception as e:
        print(f"Error: {e}")
    return info_dict

def load_data(sheet_instance, df):
    ''' This function uses the sheet connection to update data row by row.'''
    try:
        print("Loading new data into the sheet file")
        for index_, row in df.iterrows():
            if index_ < 3:
                continue
            sheet_instance.update_acell(f'G{index_+1}', row['Situação'])
            sheet_instance.update_acell(f'H{index_+1}', row['Nota para Aprovação Final'])
        print("Data loaded Successfully")
    except Exception as e:
        print(f"Error: {e}")

print("-- Process STARTED")
NUM_AULAS = 60
SHEET_INSTANCE = connection()
RECORDS_DATA = extract_data(SHEET_INSTANCE)
DF = transform_data(RECORDS_DATA)

for index, row in DF.iterrows():
    if index < 3:
        continue

    info_info_dict = calculate_grades(int(row['Faltas']), int(row['P1']), int(row['P2']),
                                      int(row['P3']))

    row['Situação'] = info_info_dict["situacao"]
    row['Nota para Aprovação Final'] = info_info_dict["naf"]

load_data(SHEET_INSTANCE, DF)
print("-- Process ENDED")
