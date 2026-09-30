from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.lower(F.trim(F.col("s.store_name"))) == "siam flagship")
    .select(
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("s.sqft"),
    )
)
