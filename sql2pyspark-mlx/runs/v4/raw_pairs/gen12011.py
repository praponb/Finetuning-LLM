from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.units") <= 11)
    .groupBy(
        F.col("ss.product_id"),
        F.when(F.col("ss.revenue") >= 3100, "high").when(F.col("ss.revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") >= 5)
)
