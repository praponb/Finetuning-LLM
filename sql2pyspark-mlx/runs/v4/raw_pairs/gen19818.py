from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("store_id").isin([2, 5, 6]) & (F.col("quantity") == 157))

inventory_stats = (
    recent_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = inventory_stats.select("last_restock_month", "quantity_range", "total_quantity")
