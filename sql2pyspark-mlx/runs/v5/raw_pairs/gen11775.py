from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([1, 7, 9]) | (F.col("ss.product_id") == 3))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.sale_date"),
        F.add_months(F.col("i.last_restock"), 6).alias("last_restock_plus_months"),
    )
    .orderBy(F.col("id").desc())
)
