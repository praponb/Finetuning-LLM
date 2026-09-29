from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.count(F.col("ss.units")).alias("units_count"))
    .orderBy(F.col("opened_year").desc())
    .limit(50)
)
