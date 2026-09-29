from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
        F.countDistinct(F.col("ss.units")).alias("unique_products"),
    )
    .orderBy("last_sale_date", F.col("unique_products").desc())
)
