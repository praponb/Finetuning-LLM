from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.year(F.col("ss.sale_date")) == 2024)
    .groupBy(
        F.when(F.col("ss.units") >= 34, "high").when(F.col("ss.units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
    )
)
