import pandas as pd

def create_delivery_dataframe():
    # Load the tables with specific columns
    # Load nota_fiscal with its primary key 'id' and 'chave'
    nf_df = pd.read_csv('Instivo_Postgres/tables/nota_fiscal.csv', usecols=['id', 'chave'])
    # Load entrega (deliveries) and include the primary key 'id'
    entrega_df = pd.read_csv('Instivo_Postgres/tables/entrega.csv', usecols=[
        'id',
        'doca_id',
        'data_agendamento',
        'tipo_veiculo',
        'tipo_carga',
        'tipo_volume',
        'quantidade_volume',
        'status'
    ])

    # Save a copy of the raw entrega data if needed
    entrega_df.to_csv('Instivo_Postgres/entrega_df_filtered.csv', index=False)

    # Load the linking table between entrega and nota_fiscal.
    # Since the file has no header, we provide column names: entrega_id, nota_fiscal_id.
    entrega_nota_df = pd.read_csv(
        'Instivo_Postgres/tables/entrega_nota_fiscal.csv', 
        header=None, 
        names=['entrega_id', 'nota_fiscal_id']
    )

    # Merge entrega with the linking table on entrega.id = entrega_nota_fiscal.entrega_id
    merged_df = pd.merge(
        entrega_df, 
        entrega_nota_df, 
        left_on='id',      
        right_on='entrega_id',
        how='inner'
    )

    # Merge the result with nota_fiscal on nota_fiscal_id = nota_fiscal.id
    merged_df = pd.merge(
        merged_df,
        nf_df,
        left_on='nota_fiscal_id',  
        right_on='id',             
        how='left',
        suffixes=('_entrega', '_nf')
    )

    # Load and merge doca vehicle types
    doca_veiculo = pd.read_csv(
        'Instivo_Postgres/tables/doca_tipo_veiculo.csv',
        usecols=['doca_id', 'tipo_veiculo']
    )
    merged_df = pd.merge(
        merged_df,
        doca_veiculo,
        on='doca_id',
        how='left',
        suffixes=('', '_doca')
    )

    # Load and merge doca carga types
    doca_carga = pd.read_csv(
        'Instivo_Postgres/tables/doca_tipo_carga.csv',
        usecols=['doca_id', 'tipo_carga']
    )
    merged_df = pd.merge(
        merged_df,
        doca_carga,
        on='doca_id',
        how='left',
        suffixes=('', '_doca')
    )

    # Reorder and select final columns
    # Here we select 'chave' from nota_fiscal along with delivery and dock info.
    final_df = merged_df[[
        'chave',
        'doca_id',
        'data_agendamento',
        'tipo_veiculo_doca',
        'tipo_carga_doca',
        'tipo_volume',
        'quantidade_volume',
        'status'
    ]]
    
    return final_df

if __name__ == '__main__':
    df = create_delivery_dataframe()
    df.to_csv('Instivo_Postgres/delivery_merged.csv', index=False)
    print(f"Merged dataset created with {len(df)} records")
    print("Sample data:")
    print(df.head()) 
