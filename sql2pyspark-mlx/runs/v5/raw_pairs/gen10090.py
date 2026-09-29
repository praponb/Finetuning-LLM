from pyspark.sql import functions as F

result = df_store_sales.withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
