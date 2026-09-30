from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-11-01")
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 260, "high").when(F.col("quantity") >= 55, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)
