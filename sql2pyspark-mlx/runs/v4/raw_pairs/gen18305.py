from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .select(F.col("stores.opened_date"), F.col("t.unique_stores"))
)
