from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("units") == 19)
    .groupBy("store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.countDistinct("units").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.unique_stores") > 8)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("t.avg_units"),
        F.col("t.unique_stores"),
    )
)
