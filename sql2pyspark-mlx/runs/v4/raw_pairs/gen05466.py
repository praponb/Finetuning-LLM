from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity") <= 110)
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 12))
)

inventory_stats = (
    base
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    inventory_stats
    .orderBy("min_quantity", "quantity_count", "product_id", "last_restock_year")
    .limit(10)
)
