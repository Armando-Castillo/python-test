from datetime import date, datetime
from numpy import int0, int16, string_
import pandas as pd
import os 

#/home/armando/colektia/clientes.csv
    
def main():
  #Read path for data source
  data_source = input("Ingrese ruta/path del data source: ")
  df = read_data(data_source)
  clean_df = clean_data(df)
  print(clean_df['due_date'])
  # df_transformed = transform_data(df)
  # load_data(df_transformed)
 
  
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
  df['email'] = df['email'].fillna(value='None')
  df['status'] = df['status'].fillna(value='None')
  df['priority'] = df['priority'].fillna(value=0)
  df['phone'] = df['priority'].fillna(value=0)
  #DATA STRUCTURE
  df['fiscal_id'] = df['fiscal_id'].astype('string')
  df['first_name'] = df['first_name'].astype('string')
  df['last_name'] = df['last_name'].astype('string')
  df['gender'] = df['gender'].astype('string')
  df['birth_date'] = pd.to_datetime(df['birth_date'])
  df['due_date'] = pd.to_datetime(df['due_date'])
  df['address'] = df['address'].astype('string')
  df['email'] = df['email'].astype('string')
  df['status'] = df['status'].astype('string')
  df['priority'] = df['priority'].astype(int16)
  df['phone'] = df['phone'].astype('string')
  return df


def transform_data(df):
  #df_clean = clean_df(df)
  clients_df = create_clientes_df(df_clean)
  return clients_df
  

def load_data(df):
  dir_path = os.path.dirname(os.path.realpath(__file__)) + '/output/'
  location_file = dir_path + 'clientes.xlsx'
  df.to_excel(location_file)
 
def create_clientes_df(df):
  clientes_df = df[["fiscal_id", "first_name", "last_name", "gender"]]
  clientes_df["birth_date"] = df["fecha_nacimiento"] 
  return clientes_df

if __name__ == '__main__':
  main()