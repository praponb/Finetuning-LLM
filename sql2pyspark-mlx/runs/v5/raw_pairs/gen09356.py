from pyspark.sql import functions as F

result = df_store_sales.select("product_id", "store_id", "units")
