from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
    )
    .orderBy("region", F.col("discount_range").desc())
    .limit(5)
)
