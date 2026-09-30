from pyspark.sql import functions as F

base = df_inventory.filter(F.col("store_id").isin([1, 3]) & (F.col("quantity") < 239))

inventory_stats = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 260, "high").when(F.col("quantity") >= 68, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = inventory_stats.select("quantity_bucket", "total_quantity", "quantity_range")
