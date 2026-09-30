from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.countDistinct(F.col("ss.units")).alias("unique_products"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
    .filter(F.col("unique_products") >= 5)
)
