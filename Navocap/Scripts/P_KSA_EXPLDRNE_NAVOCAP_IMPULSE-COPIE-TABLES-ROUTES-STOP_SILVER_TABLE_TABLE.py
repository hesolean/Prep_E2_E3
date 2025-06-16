#!/usr/bin/env python
# coding: utf-8

# ## P_KSA_EXPLDRNE_NAVOCAP_IMPULSE-COPIE-TABLES-ROUTES-STOP_SILVER_TABLE_TABLE
# 
# New notebook

# In[1]:


from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Lakehouse Table Copy") \
    .getOrCreate()

# Chemins de tables source et cible
source_table_path1 = "abfss://137898d7-9166-4c21-b066-dd2e2dd723c9@onelake.dfs.fabric.microsoft.com/d3f0d2ec-a20a-4ad4-984d-a6b7f5c9eac6/Tables/routes"
source_table_path2 = "abfss://137898d7-9166-4c21-b066-dd2e2dd723c9@onelake.dfs.fabric.microsoft.com/d3f0d2ec-a20a-4ad4-984d-a6b7f5c9eac6/Tables/stops"
target_table_path1 = "abfss://84c84e57-9f4b-4896-8968-fa2878d883be@onelake.dfs.fabric.microsoft.com/7842e0c1-a56f-4c42-bd64-ce8813413131/Tables/routes"
target_table_path2 = "abfss://84c84e57-9f4b-4896-8968-fa2878d883be@onelake.dfs.fabric.microsoft.com/7842e0c1-a56f-4c42-bd64-ce8813413131/Tables/stops"

# Lire les tables source
df1 = spark.read.format("delta").load(source_table_path1)
df2 = spark.read.format("delta").load(source_table_path2)

# Écrire les tables dans le Lakehouse cible
df1.write.format("delta").mode("overwrite").save(target_table_path1)
df2.write.format("delta").mode("overwrite").save(target_table_path2)

print("Tables copiées avec succès.")

# Arrêter la session Spark
spark.stop()

