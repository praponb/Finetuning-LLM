from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("days_since_opened_date").desc())
w2 = Window.partitionBy("city").orderBy(F.col("row_num").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("rank_in_group").desc())
w4 = Window.orderBy(F.col("days_since_opened_date").asc_nulls_last())
w5 = Window.orderBy(F.col("days_since_opened_date").asc_nulls_first())
w_part = Window.orderBy("days_since_opened_date")
w_running = Window.orderBy(F.col("days_since_opened_date").desc())
w8 = Window.orderBy(F.col("days_since_opened_date_dense_rank_3").asc_nulls_last())

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-04-23").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_stores = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest_stores
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_name", "days_since_opened_date", "row_num", "rank_in_group")
)

kept_keys = (
    deduped
    .select("id")
    .intersectAll(
        df_stores
        .filter(F.col("store_name").isNull())
        .select("id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("row_num") == 1)
    .withColumn("days_since_opened_date_dense_rank", F.dense_rank().over(w4))
    .withColumn("days_since_opened_date_dense_rank_2", F.dense_rank().over(w5))
    .withColumn("days_since_opened_date_dense_rank_3", F.dense_rank().over(w_part))
    .withColumn("days_since_opened_date_percent_rank", F.percent_rank().over(w_running))
    .withColumn("days_since_opened_date_cume_dist", F.cume_dist().over(w_running))
)

enriched = (
    with_windows
    .withColumn("days_since_opened_date_dense_rank_3_dense_rank", F.dense_rank().over(w8))
    .withColumn("days_since_opened_date_rank", F.rank().over(w5))
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("days_since_opened_date_rank") >= 9, "LARGE").otherwise("SMALL").alias("days_since_opened_date_rank_tier"),
    )
    .agg(
        F.percentile_approx("days_since_opened_date", [0.25, 0.5, 0.75]).alias("days_since_opened_date_quartiles"),
        F.avg("days_since_opened_date_dense_rank_3").alias("avg_days_since_opened_date_dense_rank_3"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("days_since_opened_date_percent_rank").alias("max_days_since_opened_date_percent_rank"),
        F.percentile_approx("days_since_opened_date_dense_rank", 0.9).alias("days_since_opened_date_dense_rank_p90"),
    )
)

result = (
    stats
    .select(
        "days_since_opened_date_rank_tier",
        "days_since_opened_date_quartiles",
        "avg_days_since_opened_date_dense_rank_3",
        "cnt",
        "distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last(), "cnt")
)
