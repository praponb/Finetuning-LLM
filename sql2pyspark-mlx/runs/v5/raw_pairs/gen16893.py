from pyspark.sql import functions as F

result = df_inventory.filter(F.col("store_id").isin([1, 5, 6]))
