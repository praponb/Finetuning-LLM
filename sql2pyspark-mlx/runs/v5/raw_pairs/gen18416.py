from pyspark.sql import functions as F

result = df_inventory.select("store_id", "last_restock")
