from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2024-12-31") & F.col("product_id").isin([9, 10]))
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") > 70)
    .orderBy("product_id")
)
