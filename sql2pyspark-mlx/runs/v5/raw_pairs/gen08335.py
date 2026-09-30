from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("last_restock").between("2023-07-23", "2024-07-29") & (F.col("store_id") == 4))
)

summary = (
    recent_inventory
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 192, "high").when(F.col("quantity") >= 120, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = summary.select("store_id", "unique_products")
