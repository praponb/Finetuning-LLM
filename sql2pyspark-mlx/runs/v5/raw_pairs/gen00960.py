from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.quantity") > 69)
    .groupBy(
        F.when(F.col("ss.revenue") >= 4200, "high").when(F.col("ss.revenue") >= 1200, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
)
