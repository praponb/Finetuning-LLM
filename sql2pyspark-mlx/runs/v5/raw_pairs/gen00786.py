from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.cost"),
    )
    .orderBy("store_name", "sqft")
)
