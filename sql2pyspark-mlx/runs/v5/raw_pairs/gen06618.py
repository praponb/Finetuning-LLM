from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.city").isin(["New York", "Bangkok"]) | (F.col("p.stock_qty") < 396))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("city").desc(), "stock_qty")
)
