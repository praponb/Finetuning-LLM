from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 6, 7, 9]))
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.min("last_restock").alias("first_last_restock"))
)
