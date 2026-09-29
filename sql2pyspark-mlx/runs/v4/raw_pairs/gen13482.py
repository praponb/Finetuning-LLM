from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-10-20")
    .groupBy(
        F.when(F.col("quantity") >= 249, "high").when(F.col("quantity") >= 102, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("avg_quantity") < 67)
)
