from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(F.col("revenue").alias("value"))
    .unionByName(df_orders.select(F.col("amount").alias("value")))
    .distinct()
)
