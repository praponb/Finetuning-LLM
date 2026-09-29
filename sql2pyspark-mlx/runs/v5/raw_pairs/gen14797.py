from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("s.opened_date")).isin([4, 7, 9]))
    .groupBy(F.col("p.category"))
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .orderBy(F.col("total_units").desc(), "category")
)
