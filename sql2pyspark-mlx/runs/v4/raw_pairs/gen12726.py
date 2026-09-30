from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
    )
)
