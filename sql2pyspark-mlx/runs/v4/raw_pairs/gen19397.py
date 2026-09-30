from pyspark.sql import functions as F

result = df_store_sales.filter(F.col("sale_date") <= "2023-10-25")
