# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 16:11:04 2024

@author: snsv
"""

import os
import pandas as pd
import fitz
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine

def find_pdfs(folder_path):
    pdf_files = []

    # Iterate through all files in the folder
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            # Check if the file is a PDF
            if file_path.lower().endswith('.pdf'):
                pdf_files.append(file_path)

    return pdf_files

def pdf_parse(path:str)->pd.DataFrame:
    pdf_document = fitz.open(path)
    date_now = datetime.now().date()
    for i in range(len(pdf_document)):
        text = pdf_document[i].get_text()
        text = text.split('\n')
        if text[0] == '[1410000] Statement of changes in equity - General Industry - Prior Year':
            temp_data = {}
            temp_data['date'] = text[1]
            temp_data['record date'] = date_now
            temp_data['saham biasa'] = text[-12]
            temp_data['tambahan modal disetor'] = text[-11]
            temp_data['cadangan lindung nilai arus kas'] = text[-10]
            temp_data['Selisih transaksi ekuitas dengan pihak non-pengendali'] = text[-9]
            temp_data['komponen transaksi ekuitas lainnya'] = text[-8]
            temp_data['saldo laba yang telah ditentukan penggunaannya'] = text[-7]
            temp_data['saldo laba yang belum ditentukan penggunaannya'] = text[-6]
            temp_data['ekuitas yang dapat diatribusikan kepada entitas induk'] = text[-5]
            temp_data['kepentingan non-pengendali'] = text[-4]
            temp_data['ekuitas'] = text[-3] 
            temp_data = pd.DataFrame([temp_data])
            temp_data['date'] = pd.to_datetime(temp_data['date'], format='%d %B %Y')
            temp_data['record date'] = pd.to_datetime(temp_data['date'], format='%d %B %Y')
            return temp_data
            
        else:
            pass


def main():
    database_path = 'MDKA equity.db'
    log_table_name = 'log'
    equity_table_name = 'equity'
    folder_path = 'sample pdf/'
    if not os.path.exists(database_path):    
        with sqlite3.connect(database_path) as connection:
            pass
        sql_engine = create_engine(f'sqlite:///{database_path}')
        log_df_dummy = {'file name':None}
        data_df_dummy = {'date': None,
                         'record date': None,
                         'saham biasa': None,
                         'tambahan modal disetor': None,
                         'cadangan lindung nilai arus kas': None,
                         'Selisih transaksi ekuitas dengan pihak non-pengendali': None,
                         'komponen transaksi ekuitas lainnya': None,
                         'saldo laba yang telah ditentukan penggunaannya': None,
                         'saldo laba yang belum ditentukan penggunaannya': None,
                         'ekuitas yang dapat diatribusikan kepada entitas induk': None,
                         'kepentingan non-pengendali': None,
                         'ekuitas': None,
                        }
        log_df_dummy = pd.DataFrame([log_df_dummy])
        data_df_dummy = pd.DataFrame([data_df_dummy])
        log_df_dummy.to_sql(log_table_name, con=sql_engine, index=False, if_exists='replace')
        data_df_dummy.to_sql(equity_table_name, con=sql_engine, index=False, if_exists='replace')
    
    #sql engine    
    sql_engine = create_engine(f'sqlite:///{database_path}')
    
    #get existing file pdf on folder
    list_pdf = find_pdfs(folder_path)
    
    # cek log record
    query = f"SELECT * FROM {log_table_name}"
    df_existing_log = pd.read_sql_query(query, sql_engine)
    not_recorded_pdf_list = [item for item in list_pdf if item not in df_existing_log['file name'].values]
    if len(not_recorded_pdf_list)<1:
        print("nothing to add")
        pass
    else:
        for item in not_recorded_pdf_list:
            df_pdf_parsed = pdf_parse(item)
            query = f"SELECT * FROM {equity_table_name} WHERE 'date' = '{df_pdf_parsed['date'].iloc[0]}'"
            df_existing_recorded = pd.read_sql_query(query, sql_engine)
            if len(df_existing_recorded)<1:
                df_pdf_parsed.to_sql(equity_table_name, con=sql_engine, index=False, if_exists='append')
                log_df = {'file name':item}
                log_df = pd.DataFrame([log_df])
                log_df.to_sql(log_table_name, con=sql_engine, index=False, if_exists='append')
                print(f'adding {item} to database')
            else:
                pass
        
if __name__ == "__main__":
    main()