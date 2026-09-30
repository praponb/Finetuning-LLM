from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 7, 10]))
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.lower(F.col("s.city")).alias("city_lower"),
    )
)
