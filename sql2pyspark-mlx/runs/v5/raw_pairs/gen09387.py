from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 229) & F.col("store_id").isin([2, 4, 5]))
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") >= 90)
    .orderBy("min_quantity")
)
