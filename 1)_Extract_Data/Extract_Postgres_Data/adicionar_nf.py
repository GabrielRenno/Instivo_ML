import pandas as pd
from pymongo import MongoClient
import numpy as np
from tqdm import tqdm

def connect_mongodb():
    print("Conectando ao MongoDB...")
    connection_string = "mongodb://ipaas:EjnuRkikGWa9gEbV@mongo.instivo.com.br:27017/ipaas"
    client = MongoClient(connection_string)
    return client['ipaas']

def load_base_data():
    print("Carregando dados do arquivo CSV...")
    return pd.read_csv('base_product_bus_sup.csv', encoding='utf-8-sig')

def get_nf_data(db, recload_ids):
    """Busca dados de notas fiscais para os recLoadIds e agrupa em listas"""
    print("Buscando dados de notas fiscais...")
    collection = db['invReceivedLoadsPrd']
    
    # Pipeline de agregação para agrupar nfKey e nfId por recLoadId
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
    try:
        df_base = load_base_data()
        print(f"Dados base carregados. Shape: {df_base.shape}")
        
        db = connect_mongodb()
        
        # Obter lista única de recLoadIds
        recload_ids = df_base['recLoadId'].unique().tolist()
        print(f"Total de recLoadIds únicos: {len(recload_ids)}")
        
        # Buscar dados de notas fiscais agrupados
        df_nf = get_nf_data(db, recload_ids)
        
        if not df_nf.empty:
            # Fazer merge dos dados
            df_enriched = df_base.merge(
                df_nf,
                on='recLoadId',
                how='left'
            )
            
            # Converter NaN para listas vazias
            df_enriched['nfKeys'] = df_enriched['nfKeys'].apply(lambda x: x if isinstance(x, list) else [])
            df_enriched['nfIds'] = df_enriched['nfIds'].apply(lambda x: x if isinstance(x, list) else [])
            
            print("\nDados enriquecidos:")
            print(f"Shape original: {df_base.shape}")
            print(f"Shape final: {df_enriched.shape}")
            
            # Mostrar estatísticas
            print("\nEstatísticas de notas fiscais:")
            print(f"Registros com notas fiscais: {df_enriched['nfKeys'].apply(len).gt(0).sum()}")
            print(f"Média de NFs por registro: {df_enriched['nfKeys'].apply(len).mean():.2f}")
            print(f"Máximo de NFs por registro: {df_enriched['nfKeys'].apply(len).max()}")
            
            # Salvar resultado
            output_file = 'base_product_bus_sup_nfkey_nfid.csv'
            df_enriched.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\nDados salvos em: {output_file}")
            
            # Mostrar alguns exemplos
            print("\nExemplos de registros com múltiplas notas fiscais:")
            multi_nf = df_enriched[df_enriched['nfKeys'].apply(len) > 1]
            if not multi_nf.empty:
                print(multi_nf[['recLoadId', 'nfKeys', 'nfIds']].head())
            else:
                print("Nenhum registro com múltiplas notas fiscais encontrado.")
            
        else:
            print("Não foi possível enriquecer os dados - sem informações de notas fiscais")
            
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")

if __name__ == "__main__":
    main()
