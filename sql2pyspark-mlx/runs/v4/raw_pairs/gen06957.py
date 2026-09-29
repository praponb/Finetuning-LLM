from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("quantity").between(169, 235) & F.col("quantity").between(66, 156))
)

quantity_bucket_summary = (
    valid_inventory
    .groupBy(
        F.when(F.col("quantity") >= 192, "high").when(F.col("quantity") >= 116, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = quantity_bucket_summary.orderBy(F.col("min_last_restock").desc())
