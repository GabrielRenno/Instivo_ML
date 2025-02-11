import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2

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
            SELECT id, doca_id, data_agendamento, tipo_veiculo, tipo_carga, tipo_volume, quantidade_volume, status 
            FROM entrega
            """, conn)
        entrega_nota_df = pd.read_sql("SELECT entrega_id, nota_fiscal_id FROM entrega_nota_fiscal", conn)
        doca_veiculo = pd.read_sql("SELECT doca_id, tipo_veiculo FROM doca_tipo_veiculo", conn)
        doca_carga = pd.read_sql("SELECT doca_id, tipo_carga FROM doca_tipo_carga", conn)
        
        conn.close()

    # --- Data Merging ---
    # 1. Merge entrega with the linking table: entrega.id = entrega_nota_fiscal.entrega_id
    merged_df = pd.merge(
        entrega_df, 
        entrega_nota_df, 
        left_on='id', 
        right_on='entrega_id', 
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

    # 3. Merge dock vehicle types (using doca_id); original entrega table may have its own tipo_veiculo.
    #    The merge adds the dock's tipo_veiculo as a new column via a suffix.
    merged_df = pd.merge(
        merged_df,
        doca_veiculo,
        on='doca_id',
        how='left',
        suffixes=('', '_doca')
    )

    # 4. Merge dock carga types (using doca_id) to add the dock's tipo_carga.
    merged_df = pd.merge(
        merged_df,
        doca_carga,
        on='doca_id',
        how='left',
        suffixes=('', '_doca')
    )

    # 5. Select and reorder final columns.
    # Here we choose:
    #   - 'chave' from nota_fiscal,
    #   - 'doca_id' and 'data_agendamento' from entrega,
    #   - 'tipo_veiculo_doca' and 'tipo_carga_doca' coming from the dock tables,
    #   - and other delivery fields.
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
    
    # Save the final merged dataset.
    final_df.to_csv('1)_Extract_Data/Extract_Postgres_Data/Data/delivery_merged.csv', index=False)
    return final_df

if __name__ == '__main__':
    df = download_and_merge_data()
    print(f"Merged dataset created with {len(df)} records")
    print("Sample data:")
    print(df.head()) 