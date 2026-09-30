from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 5, 9]) & F.month("last_restock").isin([7, 8, 10]))
)

summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = summary.select("product_id", "total_quantity")
