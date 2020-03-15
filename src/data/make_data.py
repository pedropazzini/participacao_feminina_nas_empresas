'''
Módulo para realizar as transformações nos dados para serem feitas as análises
'''

import re
import operator
from collections import defaultdict, Counter
from functools import reduce
from os import makedirs, listdir
from os.path import isfile
from unicodedata import normalize

import pandas as pd
from tqdm import tqdm
from dask.distributed import as_completed

from src.models.tree import Tree

def remover_acentos(txt):
    return normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')

def get_tree_of_names():
    '''
    Monta uma árvore com os nomes e generos para consulta.
    '''
    tree = Tree()
    df_names = get_dataset_names()
    for row in tqdm(df_names.itertuples(), total=df_names.shape[0]):
        tree.insert(row.first_name, row.classification)

    return tree

def get_dataset_names():
    '''
    Dataset com os nomes. Obtido de: https://brasil.io/dataset/genero-nomes/nomes
    '''
    return pd.read_csv(('data/raw/' +
                        'genero-nomes-33ddbca0e1c041598f4773034f257280.csv'),
                        usecols=['first_name', 'classification']
                      )

def generate_partners_gender():
    '''
    Gera os generos de cada sócio e salva no formato parquet.
    '''
    total = 100
    folder = const_folder_genders()
    makedirs(folder, exist_ok=True)
    tree = get_tree_of_names()
    for i, df_partners in tqdm(enumerate(dataset_generator()),
                            total=total):
        df_genders = get_partners_gender(df_partners, tree)
        filename = '{}genders.{}.parquet'.format(folder, i)
        df_genders.to_parquet(filename)

def generate_gender_stats_for_companys(dask_client, overwrite=True):
    '''
    Gera as estatísticas de genero para cada empresa
    '''
    total = 50000
    folder = 'data/processed/company_gender/'
    filename_company = '../CNPJ-full/data/empresas.csv'
    rows_in_file = 40754939 // total # Aprox. 0.2%
    makedirs(folder, exist_ok=True)
    for i, df_company in tqdm(enumerate(dataset_generator(filename_company,
                                                           rows_in_file)),
                                  total=total):
        filename = '{}company_genders.{}.parquet'.format(folder, i)
        if not overwrite and isfile(filename):
            continue
        # filtre active companys only
        df_company = df_company.loc[df_company['situacao'] == '02']
        if df_company.empty:
            continue
        df_company['cnpj'] = pd.to_numeric(df_company['cnpj'])
        cnpjs = set(df_company['cnpj'].values)
        df_company_gender = get_partners_by_cnpj_list(cnpjs, dask_client)
        if not df_company_gender.empty:
            df_company_gender = df_company.join(df_company_gender, on='cnpj')
            df_company_gender.to_parquet(filename)


def get_partners_gender(df_partners, tree=None):
    '''
    Retorna um dataframe com o sexo de cada partner.
    '''
    if tree is None:
        tree = get_tree_of_names()
    data = [tree.query(remover_acentos(row.nome_socio.split()[0]).upper()) for
            row in df_partners.itertuples()]
    df_partners['gender'] =  data
    df_partners['gender'].fillna('U', inplace=True) # Unkwnow gender flag
    return df_partners

def transform_line(line, sep):
    return [i[1:-1] for i in
            re.findall(r'(?:,|\n|^)("(?:(?:"")*[^"]*)*"|[^",\n]*|(?:\n|$))',
                       line)
           ]

def dataset_generator(filename='../CNPJ-full/data/socios.csv',
                               rows=258731):
    '''
    Gera diversos datasets. Os valores default fazem com que cada arquivo possua
    aproximadamente 1% da base.
    '''
    separator=','
    chuncks_counter = 0
    counter = 0
    data = []
    with open(filename, 'r') as f:
        columns = transform_line(f.readline(), separator)
        while True:
            row = f.readline()
            if not row:
                break
            data.append(transform_line(row, separator))
            counter += 1
            if counter == rows:
                index = list(range(chuncks_counter * counter,
                                   (chuncks_counter + 1) * counter))
                df_data = pd.DataFrame(data, columns=columns, index=index)
                counter = 0
                chuncks_counter += 1
                data = []
                yield df_data

def const_folder_genders():
    return 'data/raw/genders_partners/'

def get_partners_genders_from_cnpj(filename, cnpj_list):
    df_partners = pd.read_parquet(filename, columns=['cnpj', 'gender'])
    cnpj_dict = defaultdict(list)
    mask = df_partners['cnpj'].isin(cnpj_list)
    for row in df_partners.loc[mask].itertuples():
        cnpj_dict[row.cnpj].append(row.gender)

    return {k: Counter(v) for k, v in cnpj_dict.items()}


def get_partners_by_cnpj_list(cnpj_list, folder=None):
    '''
    Retorna um dicionário com os dados de cada partner de uma lista de CNPJs.
    '''
    cnpj_genders = pd.read_parquet('data/processed/cnpj_genders.parquet')
    result_dict = {}
    for cnpj in tqdm(cnpj_list):
        try:
            result_dict[cnpj] = (
                cnpj_genders.loc[cnpj, 'gender'].value_counts().to_dict())
        except KeyError:
            result_dict[cnpj] = {'F': 0, 'M': 0, 'U': 0}
        except AttributeError:
            result_dict[cnpj] = cnpj_genders.loc[cnpj].value_counts().to_dict()

    return get_gender_stats(result_dict)

def get_gender_stats(data):
    '''
    Retorna um dataframe com as contagens de gênero por CNPJ.
    '''
    df_stat = pd.DataFrame.from_dict(data, orient='index').fillna(0).astype(int)
    if 'U' not in df_stat:
        df_stat['U'] = 0 # all dataframes must have the same columns
    share_males, share_females, share_unknown, total_partners = [], [], [], []
    for row in df_stat.itertuples():
        total = row.M + row.F + row.U
        total_partners.append(total)
        share_males.append(row.M/total if total else 0)
        share_females.append(row.F/total if total else 0)
        share_unknown.append(row.U/total if total else 0)
    df_stat['total_partners'] = total_partners
    df_stat['share_M'] = share_males
    df_stat['share_F'] = share_females
    df_stat['share_U'] = share_unknown

    return df_stat

if __name__ == '__main__':
    generate_partners_gender()
    generate_gender_stats_for_companys(dask_client)
