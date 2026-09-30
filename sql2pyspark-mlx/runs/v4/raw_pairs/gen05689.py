from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("product_id").isin([2, 4]))

inventory_stats = (
    recent_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.count("*").alias("cnt"))
)

result = (
    inventory_stats
    .filter(F.col("cnt") > 9)
    .select("last_restock_month", "cnt")
    .orderBy(F.col("cnt").desc(), "last_restock_month")
    .limit(10)
)
