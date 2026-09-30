from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .filter((F.col("ss.revenue") >= 2500) & F.col("s.region").isin(["North", "East", "West"]))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
)
