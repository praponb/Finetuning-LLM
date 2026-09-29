from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("store_id").isin([4, 5]))

inventory_stats = (
    valid_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "product_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    inventory_stats
    .filter(F.col("avg_quantity") < 20)
    .select("last_restock_cohort", "product_id", "avg_quantity", "inventory_count")
)
