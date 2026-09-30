from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(
        F.when(F.col("store_sales.units") >= 31, "high").when(F.col("store_sales.units") >= 19, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc(), "units_bucket")
)
