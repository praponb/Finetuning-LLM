from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.when(F.col("ss.units") >= 31, "high").when(F.col("ss.units") >= 15, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy("total_quantity", "units_bucket")
    .limit(3)
)
