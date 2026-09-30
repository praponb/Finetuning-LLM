from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([5, 10, 12]) & (F.col("ss.sale_date") > "2025-08-11"))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("p.brand"),
    )
)
