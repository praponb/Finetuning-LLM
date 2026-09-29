from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2024-01-21") & F.col("product_id").isin([4, 7, 10]))
    .groupBy(
        "product_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 1)
    .drop("quantity_count")
)
