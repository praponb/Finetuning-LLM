from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("p.price") >= 1100) & F.col("p.price").between(400, 480))
    .select(
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
)
