from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy("last_restock_month", F.col("min_quantity").desc())
)
