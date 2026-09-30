from pyspark.sql import functions as F

result = df_stores.filter((F.length("store_name") > 11) & (F.col("region") == "East"))
