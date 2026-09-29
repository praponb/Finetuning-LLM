from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("product_id").isin([3, 8]))

summary = (
    valid_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    summary
    .filter(F.col("inventory_count") < 6)
    .select("last_restock_year", "min_last_restock", "inventory_count")
    .orderBy(F.col("min_last_restock").desc(), "last_restock_year")
    .limit(3)
)
