from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("created_at").desc())
w2 = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w_ordered = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("k").orderBy(F.col("row_seq").desc_nulls_first())
w6 = Window.partitionBy("hour_window").orderBy(F.col("utm_source_events").desc())

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("k", "attr_value"))
)

latest = (
    lines
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.first("k", ignorenulls=True).over(w_ordered).alias("first_known_k"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("created_at") < "2025-02-01")
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4))
    .withColumn("row_seq_share_2", F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4))
    .withColumn("row_seq_dense_rank", F.dense_rank().over(w5))
)

summary = (
    enriched
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.percentile_approx("row_seq_share_2", [0.25, 0.5, 0.75]).alias("row_seq_share_2_quartiles"),
        F.count_if(F.col("k") == "utm_source").alias("utm_source_events"),
    )
    .withColumn("utm_source_events_rank", F.rank().over(w6))
)

result = (
    summary
    .select(
        "hour_window",
        "activity_day",
        "utm_source_events",
        "utm_source_events_rank",
    )
)
