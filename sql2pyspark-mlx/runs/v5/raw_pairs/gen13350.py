from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([1, 2, 3]))
    .select(
        F.col("i.store_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.revenue"),
        F.round((F.col("ss.units") * 0.85), 2).alias("units_scaled"),
    )
    .orderBy(F.col("store_sale_store_id").desc())
)
