import pandas as pd

#/home/armando/colektia/clientes.csv
    
def main():
  data_source = input("Ingrese ruta/path del data source: ")
  print(data_source)

  df = pd.read_csv(data_source, sep=';')
  print(df)

  data_target = input("Ingrese ruta/path del data target: ")
  print(data_target)
  
if __name__ == '__main__':
  main()