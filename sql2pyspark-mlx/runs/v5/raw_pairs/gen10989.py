from pyspark.sql import functions as F

result = df_store_sales.select("store_id", "sale_date")
