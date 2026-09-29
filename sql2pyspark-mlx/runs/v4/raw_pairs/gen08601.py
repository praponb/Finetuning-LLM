from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 7]) & (F.col("product_id") == 2))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
    .withColumn("revenue_per_unit_2", (F.col("revenue") - F.col("units")))
)
