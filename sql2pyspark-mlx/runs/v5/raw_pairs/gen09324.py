from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .filter(F.col("ss.store_id").isin([2, 3, 5, 6]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.stock_qty"),
    )
)
