from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("p.supplier_id").isin([5, 15, 29, 36]) & (F.col("ss.revenue") < 1900))
    .select(
        F.col("ss.store_id"),
        F.col("s.opened_date"),
        F.col("p.stock_qty"),
        F.date_trunc("year", F.col("s.opened_date")).alias("opened_date_period"),
    )
)
