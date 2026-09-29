from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.region") == "West")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("id").desc())
)
