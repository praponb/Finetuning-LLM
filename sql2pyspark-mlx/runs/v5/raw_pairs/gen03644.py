from pyspark.sql import functions as F

result = df_inventory.agg(F.countDistinct("quantity").alias("unique_stores"))
