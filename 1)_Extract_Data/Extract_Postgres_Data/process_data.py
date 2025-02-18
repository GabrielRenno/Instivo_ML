from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import numpy as np

def connect_mongodb():
    connection_string = "mongodb://ipaas:EjnuRkikGWa9gEbV@mongo.instivo.com.br:27017/ipaas"
    client = MongoClient(connection_string)
    return client['ipaas']

def collection_to_dataframe(db, collection_name):
    """Converte uma collection do MongoDB para DataFrame"""
    collection = db[collection_name]
    documents = list(collection.find())
    return pd.DataFrame(documents) if documents else pd.DataFrame()

def process_load_group(group):
    """Processa cada grupo de registros"""
    group = group.sort_values('collDateTime').reset_index(drop=True)
    
    if len(group) <= 1:
        return pd.DataFrame()
        
    time_diffs = np.diff(group['collDateTime'].astype(np.int64)) / 1e9
    
    records = {
        'recLoadId': group['recLoadId'].iloc[:-1],
        'prodId': group['prodId'].iloc[:-1], 
        'busUn': group['busUn'].iloc[:-1],
        'qttcheck': group['qttChek'].iloc[:-1],
        'loadCrDateTime': group['loadCrDateTime'].iloc[:-1],
        'time': time_diffs
    }
    
    return pd.DataFrame(records)

def get_time_range(time):
    """Define a faixa de tempo para cada registro"""
    time_ranges = [
        (0, 60), (60, 300), (300, 600),
        (600, 1800), (1800, 3600), (3600, float('inf'))
    ]
    
    for start, end in time_ranges:
        if start <= time < end:
            if end == float('inf'):
                return '> 1 hora'
            return f'{start/60:.0f}-{end/60:.0f} min'
    return 'Indefinido'

def extract_supp_ids(supp_list):
    """Extrai IDs de fornecedores da lista"""
    if isinstance(supp_list, list):
        return [item['suppId'] for item in supp_list if 'suppId' in item]
    return []

def get_nf_data(db, recload_ids):
    """Busca dados de notas fiscais para os recLoadIds e agrupa em listas"""
    print("Buscando dados de notas fiscais...")
    
    collection = db['invReceivedLoadsPrd']
    
    pipeline = [
        {'$match': {'recLoadId': {'$in': recload_ids}}},
        {'$group': {
            '_id': '$recLoadId',
            'nfKeys': {'$push': '$nfKey'},
            'nfIds': {'$push': '$nfId'}
        }},
        {'$project': {
            '_id': 0,
            'recLoadId': '$_id',
            'nfKeys': 1,
            'nfIds': 1
        }}
    ]
    
    nf_docs = list(collection.aggregate(pipeline))
    
    if not nf_docs:
        print("Nenhum dado de nota fiscal encontrado!")
        return pd.DataFrame()
    
    df_nf = pd.DataFrame(nf_docs)
    print(f"Notas fiscais encontradas para {len(df_nf)} recLoadIds")
    return df_nf

def main():
    print("Iniciando processamento dos dados...")
    
    # Conectar ao MongoDB
    db = connect_mongodb()
    
    # Carregar dados das collections
    print("Carregando dados do MongoDB...")
    # Corrigir chamadas de collection_to_dataframe incluindo o parâmetro db
    df_load_verification = collection_to_dataframe(db, 'loadVerificationPrd')
    df_product = collection_to_dataframe(db, 'product')
    df_business_unit = collection_to_dataframe(db, 'businessUnit')
    df_supplier = collection_to_dataframe(db, 'supplier')
    
    # Verificar se os dados foram carregados
    print(f"Registros carregados:")
    print(f"- Load Verification: {len(df_load_verification)}")
    print(f"- Products: {len(df_product)}")
    print(f"- Business Units: {len(df_business_unit)}")
    print(f"- Suppliers: {len(df_supplier)}")
    
    if len(df_load_verification) == 0:
        print("ERRO: Nenhum dado carregado do MongoDB!")
        return
    
    # Criar amostra de teste
    print("Criando amostra de teste...")
    df_load_verification_filtered = df_load_verification.copy()
    
    # Converter datas
    df_load_verification_filtered.loc[:, 'collDateTime'] = pd.to_datetime(df_load_verification_filtered['collDateTime'])
    df_load_verification_filtered.loc[:, 'loadCrDateTime'] = pd.to_datetime(df_load_verification_filtered['loadCrDateTime'])
    
    # Processar dados em lotes
    print("Processando dados em lotes...")
    chunk_size = 100
    unique_recloads = df_load_verification_filtered['recLoadId'].unique()
    processed_dfs = []
    
    progress_bar = tqdm(range(0, len(unique_recloads), chunk_size))
    for i in progress_bar:
        chunk_recloads = unique_recloads[i:i + chunk_size]
        chunk_data = df_load_verification_filtered[df_load_verification_filtered['recLoadId'].isin(chunk_recloads)]
        
        for recload_id in chunk_recloads:
            group_data = chunk_data[chunk_data['recLoadId'] == recload_id]
            df_processed = process_load_group(group_data)
            if not df_processed.empty:
                processed_dfs.append(df_processed)
    
    # Combinar resultados
    df_base = pd.concat(processed_dfs, ignore_index=True)
    df_base['time_range'] = df_base['time'].apply(get_time_range)
    
    # Adicionar informações de produtos
    print("Adicionando informações de produtos...")
    df_base_product = df_base.merge(
        df_product[['prodId', 'familyName']],
        on='prodId',
        how='left'
    )
    
    # Adicionar informações de unidades de negócio
    print("Adicionando informações de unidades de negócio...")
    df_base_product_bus = df_base_product.merge(
        df_business_unit[['busUnId', 'busUnLegEntName']],
        left_on='busUn',
        right_on='busUnId',
        how='left'
    ).drop('busUnId', axis=1)
    
    # Renomear colunas
    df_base_product_bus = df_base_product_bus.rename(columns={
        'time': 'tempo de descarga',
        'familyName': 'Produto',
        'busUnLegEntName': 'Cliente'
    })
    
    # Adicionar informações de fornecedores
    print("Adicionando informações de fornecedores...")
    df_product['supp_ids'] = df_product['supp'].apply(extract_supp_ids)
    supp_id_to_name = dict(zip(df_supplier['suppId'], df_supplier['suppLegEntName']))
    
    def get_supplier_names(id_list):
        if isinstance(id_list, list):
            return [supp_id_to_name.get(supp_id) for supp_id in id_list if supp_id in supp_id_to_name]
        return []
    
    df_base_product_bus = df_base_product_bus.merge(
        df_product[['prodId', 'supp_ids']], 
        on='prodId',
        how='left'
    )
    
    df_base_product_bus['Fornecedores'] = df_base_product_bus['supp_ids'].apply(get_supplier_names)
    df_base_product_bus = df_base_product_bus.drop('supp_ids', axis=1)
    
    # Adicionar notas fiscais
    print("\nAdicionando informações de notas fiscais...")
    recload_ids = df_base_product_bus['recLoadId'].unique().tolist()
    df_nf = get_nf_data(db, recload_ids)
    
    if not df_nf.empty:
        # Fazer merge dos dados com notas fiscais
        df_final = df_base_product_bus.merge(
            df_nf,
            on='recLoadId',
            how='left'
        )
        
        # Converter NaN para listas vazias
        df_final['nfKeys'] = df_final['nfKeys'].apply(lambda x: x if isinstance(x, list) else [])
        df_final['nfIds'] = df_final['nfIds'].apply(lambda x: x if isinstance(x, list) else [])
        
        print("\nEstatísticas de notas fiscais:")
        print(f"Registros com notas fiscais: {df_final['nfKeys'].apply(len).gt(0).sum()}")
        print(f"Média de NFs por registro: {df_final['nfKeys'].apply(len).mean():.2f}")
        print(f"Máximo de NFs por registro: {df_final['nfKeys'].apply(len).max()}")
    else:
        df_final = df_base_product_bus
        print("Nenhuma nota fiscal encontrada para adicionar")

    # Salvar resultado
    output_file = 'base_product_bus_sup_nfkey_nfid.csv'
    print(f"\nSalvando resultado em {output_file}...")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
    print("Processamento concluído!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erro durante a execução: {str(e)}")
