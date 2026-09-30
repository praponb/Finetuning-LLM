from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([1, 3, 5]) & F.month(F.col("ss.sale_date")).isin([4, 7, 10]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.units"),
    )
)
