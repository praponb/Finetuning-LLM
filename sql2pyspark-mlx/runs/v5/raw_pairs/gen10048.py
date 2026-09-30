from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.when(F.col("ss.units") >= 43, "high").when(F.col("ss.units") >= 10, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
    .orderBy(F.col("units_bucket").desc())
    .limit(5)
)
