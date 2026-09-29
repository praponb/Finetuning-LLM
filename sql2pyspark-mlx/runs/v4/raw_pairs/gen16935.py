from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id").isin([7, 10]))
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") >= 29)
    .orderBy(F.col("quantity_range").desc())
)
