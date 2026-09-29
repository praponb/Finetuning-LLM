from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id").isin([3, 9]))
    .groupBy(
        F.when(F.col("ss.units") >= 33, "high").when(F.col("ss.units") >= 15, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc())
)
