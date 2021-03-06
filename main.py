from datetime import date, datetime
from numpy import int16
import pandas as pd
import os 
import sqlite3
    
def main():
  #Read path for data source
  data_source = input("Ingrese ruta/path del data source: ")
  df = read_data(data_source)
  
  #Clean data and returns a df
  clean_df = clean_data(df)
  
  #ETL process
  if etl_process(clean_df):
    print("ETL process complete")
  else:
    print("Algo salió mal")
 
  
def read_data(data_source):
  df = pd.read_csv(data_source, sep=';')
  return df
  

def clean_data(df):
  #DATA REMOVING    
  del df['altura'], df['peso']
  #DATA COLUMN RENAME
  df = df.rename(columns={
    'fecha_nacimiento': 'birth_date',
    'fecha_vencimiento': 'due_date',
    'deuda': 'due_balance',
    'direccion': 'address',
    'correo': 'email',
    'estatus_contacto': 'status',
    'prioridad': 'priority',
    'telefono': 'phone'
  })
  #DATA MISSING HANDLING
  df[['email', 'status']] = df[['email', 'status']].fillna(value='None')
  df[['priority', 'phone']] = df[['priority', 'phone']].fillna(value=0)
  #DATA STRUCTURE AND FORMAT
  df[['fiscal_id', 'first_name', 'last_name', 'gender', 'address', 'email', 'status', 'phone']] = df[[
    'fiscal_id', 'first_name', 'last_name', 'gender', 'address', 'email', 'status', 'phone'
  ]].astype('string')
  df['birth_date'] = pd.to_datetime(df['birth_date'])
  df['due_date'] = pd.to_datetime(df['due_date'])
  df['priority'] = df['priority'].astype(int16)
  #DATA FORMAT UPPER
  df = df.applymap(lambda x:x.upper() if type(x) == str else x)
  return df


def etl_process(df):
  #generate clients df
  clients_df = transform_clientes_df(df)
  load_data_to_xlsx(clients_df, 'clientes')
  
  #generate emails df
  emails_df = df[['fiscal_id', 'email', 'status', 'priority']]
  load_data_to_xlsx(emails_df, 'emails')
  
  #generate phones df
  phones_df = df[['fiscal_id', 'phone', 'status', 'priority']]
  load_data_to_xlsx(phones_df, 'phones')
  
  #Insert xlsx into sqlite3
  load_data_to_db('clientes')
  load_data_to_db('emails')
  load_data_to_db('phones')
  return 1
  

def transform_clientes_df(df):
  #Lambdas for aggregate funcitons
  df['age'] = df['birth_date'].apply(lambda x: get_age(x)).astype(int16)
  df['age_group'] = df['age'].apply(lambda x: get_age_group(x)).astype(int16)
  df['delinquency'] = df['due_date'].apply(lambda x: get_delinquency(x))
  clients_df = df[['fiscal_id', 'first_name', 'last_name', 'gender', 
    'birth_date', 'age', 'age_group', 'due_date', 'delinquency', 'due_balance', 'address']]
  return clients_df


def get_age(born):
  today = date.today()
  age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
  return age
  

def get_age_group(age):
    if age <= 20:
      age_group = 1
    elif age > 20 & age <= 30:
      age_group = 2
    elif age > 30 & age <= 40:
      age_group = 3
    elif age > 40 & age <= 50:
      age_group = 4
    elif age > 50 & age <= 60:
      age_group = 5
    else:
      age_group = 6
    return age_group
 
 
def get_delinquency(due_date):
  return datetime.today().day - due_date.day


def load_data_to_xlsx(df, filename):
  dir_path = os.path.dirname(os.path.realpath(__file__)) + '/output/'
  location_file = dir_path + filename + '.xlsx'
  df.to_excel(location_file, sheet_name=filename, index=False)
  return 1


def load_data_to_db(name):
  db = sqlite3.connect('database.db3')
  dir_path = os.path.dirname(os.path.realpath(__file__)) + '/output/' + name + '.xlsx'
  df = pd.read_excel(open(dir_path, 'rb'))
  df.to_sql(name, db)

if __name__ == '__main__':
  main()