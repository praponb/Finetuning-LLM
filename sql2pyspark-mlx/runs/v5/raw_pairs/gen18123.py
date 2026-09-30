from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w_ordered = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/product"))))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.initcap(F.trim("page")).alias("page_title"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "event_time",
        "duration_sec",
        "page_title",
        "row_num",
        "event_seq",
        F.min("event_seq").over(w_ordered).alias("last7_min_event_seq"),
        F.sum("event_seq").over(w_ordered).alias("last7_sum_event_seq"),
        F.count("event_seq").over(w_ordered).alias("last7_count_event_seq"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("event_time") >= F.to_timestamp(F.lit("2023-03-25 20:00:00")))
    .select("*", F.floor("last7_sum_event_seq").alias("last7_sum_event_seq_whole"))
)

base_web_events = (
    cleaned
    .filter(F.to_date("event_time") < "2024-08-01")
    .select(
        "id",
        "event_time",
        "duration_sec",
        "row_num",
        "last7_min_event_seq",
        "last7_sum_event_seq",
        "last7_sum_event_seq_whole",
        F.round((F.col("last7_sum_event_seq_whole") / F.col("event_seq")), 2).alias("last7_sum_event_seq_whole_per_event_seq"),
    )
)

matched_keys = (
    base_web_events
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2024-03-08")
        .select("id")
    )
)

kept_rows = (
    base_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.when(F.col("duration_sec") >= 367, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("last7_sum_event_seq_whole_per_event_seq", [0.25, 0.5, 0.75]).alias("last7_sum_event_seq_whole_per_event_seq_quartiles"),
        F.round(F.avg("last7_sum_event_seq"), 2).alias("avg_last7_sum_event_seq"),
        F.max("last7_sum_event_seq").alias("max_last7_sum_event_seq"),
        F.round(F.avg("last7_sum_event_seq"), 2).alias("avg_last7_sum_event_seq_2"),
    )
)
