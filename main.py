import pandas as pd
import os 

#/home/armando/colektia/clientes.csv
    
def main():
  #Read path for data source
  data_source = input("Ingrese ruta/path del data source: ")
  df_transformed = extract_data(data_source)
  print(df_transformed)
  load_data(df_transformed)
 
  
def extract_data(data_source):
  print(data_source)
  df = pd.read_csv(data_source, sep=';')
  df_transformed = transform_data(df)
  return df_transformed
  

def transform_data(df):
  return pd.DataFrame(df)
  

def load_data(df):
  dir_path = os.path.dirname(os.path.realpath(__file__)) + '/output/'
  location_file = dir_path + 'clientes.xlsx'
  df.to_excel(location_file)
  
  
if __name__ == '__main__':
  main()