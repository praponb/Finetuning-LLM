from pyspark.sql import functions as F

result = df_store_sales.select("store_id", "revenue")
