from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity").between(75, 176) | (F.col("quantity") > 220))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
)

inventory_stats = (
    recent_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    inventory_stats
    .filter(F.col("total_count") < 8)
    .orderBy("last_restock_month")
)
