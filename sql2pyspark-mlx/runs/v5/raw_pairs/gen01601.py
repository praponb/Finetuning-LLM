from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .filter(F.col("ss.product_id").isin([2, 7, 10]))
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
    .filter(F.col("units_range") > 53)
    .orderBy("category")
)
