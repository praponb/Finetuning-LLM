from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([6, 8, 10]) | F.col("s.city").isin(["New York", "Berlin"]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
