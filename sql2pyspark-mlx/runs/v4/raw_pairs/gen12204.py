from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("s.store_name"),
        F.col("p.brand"),
    )
    .orderBy("brand", "store_name")
)
