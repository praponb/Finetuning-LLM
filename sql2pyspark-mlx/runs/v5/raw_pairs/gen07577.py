from pyspark.sql import functions as F

result = df_inventory.select("id", "last_restock")
