from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("s.opened_date") <= "2025-08-01")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.when(F.col("s.city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
    )
)
