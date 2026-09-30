from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5, 6]) & F.col("store_id").isin([4, 6]))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "product_id",
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)
