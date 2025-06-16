#!/usr/bin/env python
# coding: utf-8

# ## P_KSA_EXPLDRNE_NAVOCAP_IMPULSE_GOLD_NTB_TABLE_CSV
# 
# New notebook

# # Récupération des données navocap

# In[ ]:


from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
from pyspark.sql.functions import current_date, date_format, col, lit, date_sub
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


# In[ ]:


# Initialisation de Spark
spark = SparkSession.builder.appName("SAE File Generation").getOrCreate()

# Création du df pour la table et le fichier final
df = spark.sql("""
-- Génération du fichier SAE
SELECT DISTINCT
    date_format(to_date(s.Date, 'dd/MM/yyyy'), 'yyyy-MM-dd') AS date_exploitation,
    s.Vehicule AS vehicle_id,
    s.IndicePassageArret AS actual_stop_sequence,
    s.NumeroCourse AS actual_trip_id,
    s.NumeroCourse AS trip_id,
    '' AS given_path_id,
    r.route_id AS route_id,
    CASE
        WHEN s.Sens = 1 THEN 0
        WHEN s.Sens = 2 THEN 1
        ELSE s.Sens
    END AS direction_id,
    st.stop_id AS stop_id,
    CASE
        WHEN date_format(to_timestamp(s.HoraireReelEntreeZoneArret, 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') IS NULL THEN date_format(to_timestamp(s.HoraireArriveeApplicable, 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')
        ELSE date_format(to_timestamp(s.HoraireReelEntreeZoneArret, 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')
    END AS actual_arrival_time,
    CASE
        WHEN date_format(to_timestamp(s.HoraireReelSortieZoneArret, 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') IS NULL THEN date_format(to_timestamp(s.HoraireDepartApplicable, 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')
        ELSE date_format(to_timestamp(s.HoraireReelSortieZoneArret, 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')
    END AS actual_departure_time,
    '' AS actual_distance
FROM navocap s
JOIN routes r ON r.shortname = s.Ligne
JOIN stops st ON st.stop_name = s.Arret
""")

# Choisissez votre date de référence
#d_day = datetime(2025, 2, 11)  # Exemple de date au format année, mois, jour

# Calculer la date d'hier par rapport à d_day
#date_yesterday = d_day - timedelta(days=1)
date_yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# Formater la date pour le nom de fichier
date_yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

# Filtrer le DataFrame sur la colonne 'date_exploitation'
df_filtered = df.filter(col('date_exploitation') == lit(date_yesterday))

# Afficher le DataFrame filtré
#df_filtered.show()

# Convertir le DataFrame Spark en DataFrame Pandas
pandas_df = df_filtered.toPandas()

# Enregistrer en CSV
filename = "passages_sae_" + date_yesterday_str + ".csv"
pandas_df.to_csv(filename, index=False)

# Convertir le DataFrame Pandas en CSV buffer
csv_buffer = StringIO()
pandas_df.to_csv(csv_buffer, sep=';', index=False)

# Configuration Azure
container_name = "expldrne"
output_path = "PROD/K6201/IMPULSE/SAE/"+filename

blob_service_client = BlobServiceClient(
    account_url=f"https://pksadrnedatalakefrcsto01.blob.core.windows.net",
    credential=secret
)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_path)

# Chagement du fichier sur Azure
blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

