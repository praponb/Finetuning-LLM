from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2023-11-01") & F.col("product_id").isin([9, 10]))
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), F.col("avg_quantity").desc())
)
