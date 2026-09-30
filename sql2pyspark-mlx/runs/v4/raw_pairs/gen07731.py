from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-10", "2025-05-01") & F.col("quantity").between(159, 200))
)

inventory_stats = (
    recent_inventory
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    inventory_stats
    .select("product_id", "last_restock_month", "last_last_restock", "min_last_restock")
    .orderBy("last_restock_month")
)
