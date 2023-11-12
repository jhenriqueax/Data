import dask.dataframe as dd
import os
from dask.diagnostics import ProgressBar


def read(directory_path, pattern='.csv', usecols=None):
    files = [file for file in os.listdir(directory_path) if file.endswith(pattern)]
    ddf_list = [dd.read_csv(os.path.join(directory_path, file), usecols=usecols) for file in files]
    return dd.concat(ddf_list)


def merge(left_ddf, right_ddf, on_column='row1'): #coluna para fazer o join
    return left_ddf.merge(right_ddf, on=on_column, how='inner') #tipo de join (inner)


def toCSV(ddf, output_path, filename):
    ddf.to_csv(os.path.join(output_path, filename), index=False, single_file=True)


def process_data(tab01, tab02, output):
    with ProgressBar():
        #colunas que deseja carregar 
        Colunas_tabela01 = ['row1', 'row2'] 
        Colunas_tabela02 = ['row1', 'row2', 'row3']

        print("Lendo as tabelas...")
        input_ddf = read(tab01, usecols=Colunas_tabela01)
        join_ddf = read(tab02, usecols=Colunas_tabela02)
        print("Filtrando as tabelas...")
        filtro = input_ddf[input_ddf['row1'] == 'name'] #filtra tabela 1 com base em um valor escolhido
        filtro = filtro.drop_duplicates(subset='row1') #retirar elementos duplicados pelo nome definido
        print("Realizando os joins das tabelas...")
        join = merge(filtro, join_ddf)
        print("Convertendo para CSV...")
        toCSV(join, output, filename)


Dir01 = '/Users/jhenriqueax/Desktop/dir1'
Dir02 = '/Users/jhenriqueax/Desktop/dir2'
filename = '01_2023.csv'
Output = '/Users/jhenriqueax/Desktop/Resultado'


process_data(Dir01, Dir02, Output)
