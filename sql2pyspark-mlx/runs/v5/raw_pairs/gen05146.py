from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.upper(F.trim(F.col("s.store_name"))) == "MALL CENTRAL")
    .select(
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
    )
    .orderBy("brand")
)
