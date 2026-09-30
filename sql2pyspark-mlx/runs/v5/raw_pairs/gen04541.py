from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("p.cost"),
        F.concat(F.col("p.product_name"), F.lit(", "), F.col("p.brand")).alias("product_name_brand"),
    )
)
