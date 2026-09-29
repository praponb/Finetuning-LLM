from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]) & (F.col("product_id") == 4) & F.col("store_id").isin([2, 5]))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
)
