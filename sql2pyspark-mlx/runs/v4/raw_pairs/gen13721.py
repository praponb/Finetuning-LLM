from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy("total_quantity", "store_id", "last_restock_cohort")
    .limit(10)
)
