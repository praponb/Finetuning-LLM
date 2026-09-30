from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.store_id"),
        F.round((F.col("ss.units") * 0.85), 2).alias("units_scaled"),
    )
    .orderBy(F.col("units_scaled").desc(), F.col("quantity").desc())
)
