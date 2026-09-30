from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.avg_units") >= 43)
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("t.stddev_units"),
        F.col("t.avg_units"),
    )
    .orderBy("city")
)
