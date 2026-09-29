from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w5 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("island_id").desc())
w_running = Window.partitionBy("store_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.partitionBy("store_id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-09-06").cast("date"))
    .withColumn("last_restock_period", F.date_trunc("MONTH", "last_restock"))
)

enriched = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w).alias("quantity_bucket"),
        F.ntile(10).over(w2).alias("quantity_bucket_2"),
        F.avg("quantity").over(w3).alias("quantity_avg_5"),
    )
)

prev_events = (
    enriched
    .withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w4))
)

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_inventory = (
    flagged
    .select(
        "id",
        "store_id",
        "last_restock",
        "last_restock_period",
        "quantity_bucket_2",
        "quantity_avg_5",
        F.sum("is_new_burst").over(w5).alias("island_id"),
    )
)

latest = (
    sessions_inventory
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    latest
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity") <= 187)
        .select("store_id", "id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("quantity_avg_5_count_30d", F.count("quantity_avg_5").over(w_running))
    .withColumn("island_id_rank", F.rank().over(w6))
    .withColumn("store_id_stddev_samp_quantity_avg_5", F.stddev_samp("quantity_avg_5").over(w8))
)

result = (
    with_windows
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max("last_restock_period").alias("last_last_restock_period"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("num_rows") > 1)
)
