from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("avg_quantity") < 180)
    .select("store_id", "last_restock_cohort", "avg_quantity", "num_inventory")
    .orderBy(F.col("store_id").desc())
)
