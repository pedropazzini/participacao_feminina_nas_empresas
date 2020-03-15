'''
Method tha makes some analyses of the cnae vs female on companies.
'''

import re
from collections import defaultdict
from os import listdir


import pandas as pd
import dask.dataframe as dd
from dask.distributed import as_completed
from dask.distributed import Client
from tqdm import tqdm

def extract_interest_cnae(value, init_pos, end_pos):
    '''
    Extract the part of the cnae definied by the positions
    '''
    return int(str(value)[init_pos:end_pos])

def get_share(row, column_name):
    '''
    Get possible capital share of a gender in a company.
    '''
    return int(row['capital_social']) * row[column_name]

def get_stats_of_cnae_id(df_cnae_id):
    '''
    Return some stats os a single cnae id.
    '''
    genders = ['M', 'F', 'U']
    counter = defaultdict(int)
    for _, row in df_cnae_id.iterrows():
        for gender in genders:
            counter[gender] += row[gender]
            col_name = 'share_' + gender
            counter['share_capital_' + gender] = get_share(row, col_name)
    return counter

def get_stats_by_cnae(filename, cnae_positions):
    '''
    Analyze the gender stats for the gender per cnae (Código Nacional de
    Atividade Empresarial).
    '''
    cols_in = ['M', 'F', 'U', 'share_M', 'share_F', 'share_U', 'capital_social',
              'cnae_fiscal']
    df_company_gender = pd.read_parquet(filename, columns=cols_in)
    df_company_gender['cnae_id'] = df_company_gender['cnae_fiscal'].apply(
        lambda x: extract_interest_cnae(x, *cnae_positions))
    cnae_stats = {}
    for cnae_id, df_cnae_id in df_company_gender.groupby('cnae_id'):
        cnae_stats[cnae_id] = get_stats_of_cnae_id(df_cnae_id)

    return pd.DataFrame.from_dict(cnae_stats, orient='index')

def join_two_results_of_cnae(df_1, df_2):
    '''
    Join two cnae analisys of cnae.
    '''
    df_concat = pd.concat((df_1, df_2))
    return df_concat.groupby(df_concat.index).sum()

def main(cnae_positions=(0, 2), scheduler_file='scheduler.json'):
    '''
    Main function that analyses the cnaes. The default value is for the "CNAE
    Seção".
    '''
    client = Client(scheduler_file=scheduler_file)
    client.upload_file('dist/src-0.1.0-py3.8.egg')
    folder = 'data/processed/company_gender/'
    regex = r'.*\.parquet$'
    files = [folder + i for i in listdir(folder) if re.match(regex, i)]
    futures = [client.submit(get_stats_by_cnae, i, cnae_positions) for i in
               files]
    last_result = None
    for future in tqmd(as_completed(futures), total=len(futures)):
        if last_result is None:
            last_result = future.result()
        else:
            last_result = join_two_results_of_cnae(last_result, future.result())
    return last_result
