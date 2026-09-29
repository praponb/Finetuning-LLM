from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.city").isin(["London", "Singapore", "Tokyo"]) & F.col("ss.units").between(11, 33))
    .select(
        F.col("ss.revenue"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.supplier_id"),
    )
    .orderBy("revenue")
)
