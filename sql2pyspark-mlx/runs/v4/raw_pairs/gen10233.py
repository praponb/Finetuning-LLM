from pyspark.sql import functions as F

result = df_products.select("id", "stock_qty", "launch_date")
