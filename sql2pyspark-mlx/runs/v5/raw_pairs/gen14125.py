from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 3, 10]) | F.col("product_id").isin([2, 7]))
)

summary = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    summary
    .filter(F.col("avg_quantity") > 220)
    .select("store_id", "avg_quantity")
)
