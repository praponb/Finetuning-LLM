from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter((F.col("s.sqft") > 9267) & F.col("s.store_name").like("Ol%"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("s.region"),
        F.col("p.price"),
        F.col("p.cost"),
        F.substring(F.col("p.product_name"), 1, 2).alias("product_name_prefix"),
    )
)
