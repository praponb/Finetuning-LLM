from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .select(
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("s.city"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
    )
)
