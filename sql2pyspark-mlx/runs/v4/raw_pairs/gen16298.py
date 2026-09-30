from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("store_id") == 1) & F.col("product_id").isin([1, 3, 5, 7]) & F.col("product_id").isin([2, 8]))
)

agg = (
    base
    .groupBy(
        "product_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    agg
    .select("product_id", "last_restock_cohort", "avg_quantity", "stddev_quantity")
    .orderBy(F.col("last_restock_cohort").desc())
)
