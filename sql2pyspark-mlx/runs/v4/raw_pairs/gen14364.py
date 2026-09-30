from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7]))
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("ss.max_quantity"),
    )
    .orderBy(F.col("city").desc())
)
