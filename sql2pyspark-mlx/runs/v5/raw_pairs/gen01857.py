from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 121)
    .groupBy(
        "store_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 68)
    .orderBy(F.col("avg_quantity").desc(), "store_id")
)
