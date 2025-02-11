import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2
import os

# --------------------------------------------------------
#  Update these variables as needed
# --------------------------------------------------------
BASTION_HOST = '3.131.159.41'
BASTION_USER = 'ec2-user'
BASTION_KEY  = '1)_Extract_Data/Extract_Postgres_Data/Secrets/instivo-rds-prd.pem'  # Path to your .pem key

RDS_ENDPOINT = 'instivo-db-prd-cluster.cluster-cirzmpiapyhg.us-east-2.rds.amazonaws.com'
RDS_PORT     = 5432
RDS_DB       = 'db_instivo_agenda_entrega_prd'
RDS_USER     = 'aimana'
RDS_PASS     = 'Test1234'

def download_and_merge_data():
    """
    Opens an SSH tunnel to the RDS instance and downloads the necessary tables:
       - nota_fiscal (invoices) [id, chave]
       - entrega (deliveries) [id, doca_id, data_agendamento, tipo_veiculo, tipo_carga, tipo_volume, quantidade_volume, status]
       - entrega_nota_fiscal (linking table) [entrega_id, nota_fiscal_id]
       - doca_tipo_veiculo (dock vehicle types) [doca_id, tipo_veiculo]
       - doca_tipo_carga (dock carga types) [doca_id, tipo_carga]
    Then merges them according to the business logic and saves the final dataframe to delivery_merged.csv
    """
    # Start SSH tunnel through the bastion host.
    # Establish an SSH tunnel to the bastion
    with SSHTunnelForwarder(
        (BASTION_HOST, 22),
        ssh_username=BASTION_USER,
        ssh_pkey=BASTION_KEY,
        remote_bind_address=(RDS_ENDPOINT, RDS_PORT)
    ) as tunnel:
        
        # Connect to PostgreSQL via the SSH tunnel
        conn = psycopg2.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=RDS_USER,
            password=RDS_PASS,
            dbname=RDS_DB
        )
        
        # Read the tables using SQL queries.
        # Note: adjust the queries if you need further filtering.
        nf_df = pd.read_sql("SELECT id, chave FROM nota_fiscal", conn)
        entrega_df = pd.read_sql(
            """
            SELECT id as entrega_id, doca_id, data_agendamento, veiculo_id, tipo_veiculo, tipo_carga, motorista_id, tipo_volume, quantidade_volume, status 
            FROM entrega
            """, conn)
        entrega_nota_df = pd.read_sql("SELECT entrega_id, nota_fiscal_id FROM entrega_nota_fiscal", conn)
        doca_veiculo = pd.read_sql("SELECT doca_id, tipo_veiculo FROM doca_tipo_veiculo", conn)
        doca_carga = pd.read_sql("SELECT doca_id, tipo_carga FROM doca_tipo_carga", conn)
        entrega_pedido_df = pd.read_sql("SELECT entrega_id, numero_pedido FROM entrega_pedido", conn)
        doca_caracteristica = pd.read_sql("SELECT doca_id, caracteristica FROM doca_caracteristica", conn)
        items_df = pd.read_sql("SELECT nota_fiscal_id, descricao, tipo_embalagem, quantidade_vendida_embalagem, valor_embalagem FROM item", conn)
        conn.close()

    # --- Data Merging ---
    # 1. Merge entrega with the linking table: entrega.id = entrega_nota_fiscal.entrega_id
    merged_df = pd.merge(
        entrega_df, 
        entrega_nota_df, 
        on='entrega_id', 
        how='inner'
    )

    # 2. Merge the result with nota_fiscal on: entrega_nota_fiscal.nota_fiscal_id = nota_fiscal.id
    merged_df = pd.merge(
        merged_df,
        nf_df,
        left_on='nota_fiscal_id',
        right_on='id',
        how='left',
        suffixes=('_entrega', '_nf')
    )

    # 3. Aggregate dock vehicle types as a list for each doca_id.
    #    This produces a new column "tipo_veiculo_doca" containing a list of vehicles available for that dock.
    doca_veiculo_list = doca_veiculo.groupby('doca_id')['tipo_veiculo'].apply(list).reset_index()
    doca_veiculo_list.rename(columns={'tipo_veiculo': 'tipo_veiculo_doca'}, inplace=True)
    merged_df = pd.merge(
        merged_df,
        doca_veiculo_list,
        on='doca_id',
        how='left'
    )

    # 4. Aggregate dock carga types as a list for each doca_id from SQL.
    doca_carga_list = doca_carga.groupby('doca_id')['tipo_carga'].apply(list).reset_index()
    doca_carga_list.rename(columns={'tipo_carga': 'tipo_carga_doca'}, inplace=True)
    merged_df = pd.merge(
        merged_df,
        doca_carga_list,
        on='doca_id',
        how='left'
    )

    # 4.5. Merge entrega_pedido to get numero_pedido using entrega_id.
    merged_df = pd.merge(
        merged_df,
        entrega_pedido_df,
        on='entrega_id',
        how='left'
    )

    # 4.6. Merge doca_caracteristica to get the caracteristica for each dock using doca_id.
    merged_df = pd.merge(
        merged_df,
        doca_caracteristica,
        on='doca_id',
        how='left'
    )

    # 4.7. Merge items to get aggregated item details from the table item.
    items_agg = items_df.groupby('nota_fiscal_id').agg({
        'descricao': lambda x: ', '.join(x),
        'tipo_embalagem': lambda x: ', '.join(x.astype(str)),
        'quantidade_vendida_embalagem': lambda x: ', '.join(x.astype(str)),
        'valor_embalagem': lambda x: ', '.join(x.astype(str))
    }).reset_index()
    items_agg.rename(columns={'descricao': 'descricao_itens'}, inplace=True)
    merged_df = pd.merge(
        merged_df,
        items_agg,
        on='nota_fiscal_id',
        how='left'
    )

    # 5. Select and reorder final columns.
    final_df = merged_df[[
        'entrega_id',
        'chave',
        'numero_pedido',
        'descricao_itens',
        'tipo_embalagem',
        'quantidade_vendida_embalagem',
        'valor_embalagem',
        'doca_id',
        'caracteristica',
        'data_agendamento',
        'veiculo_id',
        'tipo_veiculo',
        'motorista_id',
        'tipo_veiculo_doca',
        'tipo_carga',        # from the entrega table
        'tipo_carga_doca',   # aggregated from doca_tipo_carga SQL
        'tipo_volume',
        'quantidade_volume',
        'status'
    ]]
    
    # Rename columns to match the desired output
    final_df.rename(columns={
        'entrega_id': 'Entrega ID',
        'chave': 'NFkey',
        'numero_pedido': 'Número do Pedido',
        'descricao_itens': 'Descrição dos Itens',
        'tipo_embalagem': 'Tipo de Embalagem',
        'quantidade_vendida_embalagem': 'Quantidade Vendida Embalagem',
        'valor_embalagem': 'Valor Embalagem',
        'doca_id': 'Doca ID',
        'caracteristica': 'Característica_Doca',
        'data_agendamento': 'Data de Agendamento',
        'veiculo_id': 'Veículo ID',
        'tipo_veiculo': 'Tipo de Veículo',
        'motorista_id': 'Motorista ID',
        'tipo_veiculo_doca': 'Tipo de Veículo_Doca',
        'tipo_carga': 'Tipo de Carga',
        'tipo_carga_doca': 'Tipo de Carga_Doca',
        'tipo_volume': 'Tipo de Volume',    
        'quantidade_volume': 'Quantidade de Volume',
        'status': 'Status'
    }, inplace=True)
    


    # Save the final merged dataset.
    final_df.to_csv('1)_Extract_Data/Extract_Postgres_Data/Data/data_frame_postgres.csv', index=False)
    return final_df

if __name__ == '__main__':
    df = download_and_merge_data()
    print(f"Merged dataset created with {len(df)} records")
    print("Sample data:")
    print(df.head()) 
