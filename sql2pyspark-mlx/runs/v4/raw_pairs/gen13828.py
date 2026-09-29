from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.last_restock") > "2023-03-01")
    .groupBy(F.col("ss.product_id"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
)
