from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock") >= "2024-05-13")
        .select("id", "store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("cnt"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("cnt") > 1)
)
