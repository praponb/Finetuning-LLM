from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.max(F.col("ss.sale_date")).alias("last_sale_date"))
)
