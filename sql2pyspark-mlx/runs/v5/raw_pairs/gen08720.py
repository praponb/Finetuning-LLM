from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter((F.col("ss.units") > 25) & (F.col("ss.sale_date") > "2023-10-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.price"),
        F.substring(F.col("s.city"), 1, 4).alias("city_prefix"),
    )
    .orderBy(F.col("price").desc(), "city_prefix")
)
