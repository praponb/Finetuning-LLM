from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.revenue") <= 1000)
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.min(F.col("ss.units")).alias("min_units"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy("brand", F.col("max_revenue").desc())
)
