from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("S.quantity_count"),
        F.col("S.max_quantity"),
    )
    .orderBy(F.col("city").desc(), "id")
    .limit(10)
)
