#!/usr/bin/env python
# coding: utf-8

# ## P_KSA_EXPLDRNE_NAVOCAP_IMPULSE_SILVER_NTB_CSV_TABLE
# 
# New notebook

# # Copie de tous les csv du lkh vers les tables sans doublons

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import datetime
from py4j.java_gateway import java_import

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Lakehouse Data Deduplication") \
    .getOrCreate()

# Importer les classes FileSystem et Path de Hadoop
java_import(spark._jvm, "org.apache.hadoop.fs.*")

# Définir le chemin du dossier
folder_path = "abfss://84c84e57-9f4b-4896-8968-fa2878d883be@onelake.dfs.fabric.microsoft.com/7842e0c1-a56f-4c42-bd64-ce8813413131/Files"
hdfs_path = spark._jvm.Path(folder_path)
fs = hdfs_path.getFileSystem(spark._jsc.hadoopConfiguration())

# CHANGER LA VALEUR 7 POUR LE PREMIER RUN POUR AJOUTER TOUS LES FICHIERS DANS LA TABLE
# Définir la date de référence pour les fichiers de moins de 7 jours
reference_date = datetime.datetime.now() - datetime.timedelta(days=24)

# Lister les fichiers récents
file_statuses = fs.listStatus(hdfs_path)
recent_files = [status.getPath().toString() for status in file_statuses if datetime.datetime.fromtimestamp(status.getModificationTime() / 1000) > reference_date]

# Lire les fichiers récents
if recent_files:
    df = spark.read.csv(recent_files, header=True, inferSchema=True, sep=';')
    # Supprimer les doublons
    df_no_duplicates = df.dropDuplicates()

    # Spécifier le chemin de la table Delta dans le Lakehouse
    delta_table_path = "abfss://84c84e57-9f4b-4896-8968-fa2878d883be@onelake.dfs.fabric.microsoft.com/7842e0c1-a56f-4c42-bd64-ce8813413131/Tables/navocap"

    # Vérifier si la table Delta existe déjà
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        # Charger la table existante
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        
        # Faire un merge pour enlever les doublons entre la source et la table cible
        delta_table.alias("target").merge(
            df_no_duplicates.alias("source"),
            "target.Date = source.Date AND target.MatriculeACC = source.MatriculeACC AND target.HoraireReelEntreeZoneArret = source.HoraireReelEntreeZoneArret AND target.HoraireDepartApplicable = source.HoraireDepartApplicable AND target.TempsEchangePassager = source.TempsEchangePassager"
        ).whenNotMatchedInsertAll().execute()
        print("merge")
    else:
        # Si la table n'existe pas, créez-la
        df_no_duplicates.write.format("delta").save(delta_table_path)
        print("new table")

    print("That's all folks!")

# Arrêter la session Spark
spark.stop()


