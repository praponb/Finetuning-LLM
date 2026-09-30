from pyspark.sql import functions as F

result = df_store_sales.filter((F.col("units") != 22) & F.col("revenue").between(1800, 1900))
