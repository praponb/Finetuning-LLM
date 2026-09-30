from pyspark.sql import functions as F

result = df_products.select("id", "tags", "launch_date", "supplier_id")
