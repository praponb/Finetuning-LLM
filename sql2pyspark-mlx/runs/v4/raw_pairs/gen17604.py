from pyspark.sql import functions as F

result = df_store_sales.select("id", "units", "sale_date", "store_id")
