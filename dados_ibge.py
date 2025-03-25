import requests
import pandas as pd
import psycopg2
import os
from time import sleep

# Parâmetros do banco
usuario = "moises"
senha = "12345"
host = "localhost"
porta = "5432"
banco = "MeuBancoTeste"

# Lista das UFs do Brasil
ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
       'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
       'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

# Diretórios de dados
os.makedirs('dados/parquet', exist_ok=True)
os.makedirs('dados/csv', exist_ok=True)

# Função auxiliar
def safe_str(val):
    return str(val) if pd.notnull(val) else None

# Coleta e salvamento
for sigla in ufs:
    url = f"https://servicodados.ibge.gov.br/api/v1/bngb/uf/{sigla}/nomesgeograficos"
    print(f"Coletando dados da UF: {sigla}")
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erro ao buscar {sigla}: {response.status_code}")
        continue

    dados = response.json()
    if not dados.get("features"):
        print(f"Nenhum dado para UF {sigla}")
        continue

    df = pd.json_normalize(dados["features"])

    # Renomear colunas
    df = df.rename(columns={
        'properties.idNomebngb': 'id',
        'properties.nomeGeografico': 'nome_geografico',
        'properties.geocodigo': 'geocodigo',
        'properties.termoGenerico': 'termo_generico',
        'properties.termoEspecifico': 'termo_especifico',
        'properties.conectivo': 'conectivo',
        'properties.categoria': 'categoria',
        'properties.classe': 'classe',
        'properties.escalaOcorrencia': 'escala_ocorrencia',
        'properties.statusValidacao': 'status_validacao',
        'properties.nivelValidacao': 'nivel_validacao',
        'properties.sustentacaoValidacao': 'sustentacao_validacao',
        'properties.dataValidacao': 'data_validacao',
        'properties.dataPublicacao': 'data_publicacao',
        'properties.escalaOrigemGeometria': 'escala_origem_geometria',
        'properties.latitude': 'latitude',
        'properties.longitude': 'longitude',
        'properties.latitudeGMS': 'latitude_gms',
        'properties.longitudeGMS': 'longitude_gms'
    })

    # Conversão de datas
    df["data_validacao"] = pd.to_datetime(df["data_validacao"], format="%d-%m-%Y", errors="coerce")
    df["data_publicacao"] = pd.to_datetime(df["data_publicacao"], format="%d-%m-%Y", errors="coerce")

    # Adiciona a UF
    df["uf"] = sigla

    # Remover colunas do tipo lista/inconsistentes antes do Parquet
    df = df.drop(columns=[col for col in df.columns if col.startswith("geometry.")], errors="ignore")

    # Salva em arquivos
    df.to_csv(f'dados/csv/{sigla}_nomes_geograficos.csv', index=False)
    df.to_parquet(f'dados/parquet/{sigla}_nomes_geograficos.parquet', index=False)

    print(f"✅ Dados da UF {sigla} salvos.")
    sleep(1)

# Etapa de inserção no banco

# Conecta ao banco
conn = psycopg2.connect(
    dbname=banco,
    user=usuario,
    password=senha,
    host=host,
    port=porta
)
cursor = conn.cursor()

# Cria tabela (se necessário)
cursor.execute("DROP TABLE IF EXISTS nome_geografico;")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS nome_geografico (
        id INTEGER PRIMARY KEY,
        nome_geografico TEXT,
        geocodigo TEXT,
        termo_generico TEXT,
        termo_especifico TEXT,
        conectivo TEXT,
        categoria TEXT,
        classe TEXT,
        escala_ocorrencia TEXT,
        escala_origem_geometria TEXT,
        status_validacao TEXT,
        nivel_validacao TEXT,
        sustentacao_validacao TEXT,
        data_validacao DATE,
        latitude FLOAT,
        longitude FLOAT,
        latitude_gms TEXT,
        longitude_gms TEXT,
        data_publicacao DATE,
        uf TEXT
    );
""")
conn.commit()

# Para cada arquivo CSV, insere no banco
for sigla in ufs:
    csv_path = f'dados/csv/{sigla}_nomes_geograficos.csv'
    if not os.path.exists(csv_path):
        continue

    df = pd.read_csv(csv_path, parse_dates=["data_validacao", "data_publicacao"])

    for row in df.itertuples(index=False):
        try:
            cursor.execute("""
                INSERT INTO nome_geografico (
                    id, nome_geografico, geocodigo, termo_generico, termo_especifico,
                    conectivo, categoria, classe, escala_ocorrencia, escala_origem_geometria,
                    status_validacao, nivel_validacao, sustentacao_validacao, data_validacao,
                    latitude, longitude, latitude_gms, longitude_gms, data_publicacao, uf
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (id) DO NOTHING;
            """, (
                row.id,
                safe_str(row.nome_geografico),
                safe_str(row.geocodigo),
                safe_str(row.termo_generico),
                safe_str(row.termo_especifico),
                safe_str(row.conectivo),
                safe_str(row.categoria),
                safe_str(row.classe),
                safe_str(row.escala_ocorrencia),
                safe_str(row.escala_origem_geometria),
                safe_str(row.status_validacao),
                safe_str(row.nivel_validacao),
                safe_str(row.sustentacao_validacao),
                row.data_validacao.date() if pd.notnull(row.data_validacao) else None,
                row.latitude,
                row.longitude,
                safe_str(row.latitude_gms),
                safe_str(row.longitude_gms),
                row.data_publicacao.date() if pd.notnull(row.data_publicacao) else None,
                row.uf
            ))
        except Exception as e:
            print(f"Erro ao inserir ID {row.id}: {e}")

    conn.commit()
    print(f"Inseridos dados da UF {sigla} no banco.")

cursor.close()
conn.close()
print("Processo concluído com sucesso.")
