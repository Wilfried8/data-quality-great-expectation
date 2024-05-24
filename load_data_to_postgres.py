import pandas as pd
from sqlalchemy import create_engine

# Chemin vers le fichier Parquet
parquet_file = './data/793866443_restaurants-en-loire-atlantique@loireatlantique.parquet'

# Lire le fichier Parquet
df = pd.read_parquet(parquet_file)
#print(df.head(3))
#print(df.columns)


#Paramètres de connexion à PostgreSQL
db_username = 'postgres'
db_password = 'postgres'
db_host = 'localhost'
db_port = '5432'
db_name = 'dataquality'

# Créer l'URL de connexion
db_url = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

# Créer un moteur SQLAlchemy
engine = create_engine(db_url)

# Nom de la table où les données seront insérées
table_name = 'RestaurantsEnLoireAtlantique'

# Charger les données dans PostgreSQL
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Les données ont été chargées dans la table {table_name} de la base de données {db_name}.")
