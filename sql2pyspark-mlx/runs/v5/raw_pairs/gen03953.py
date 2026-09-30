from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").asc_nulls_first())
w2 = Window.partitionBy("session_id").orderBy("duration_sec_rounded", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded_p90").desc())

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-08-06").cast("date"))
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

cleaned_web_events = (
    cleaned
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_time_day",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)

deduped = (
    cleaned_web_events
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "session_id", "event_type", "event_time_day", "duration_sec_rounded")
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

kept_keys = (
    ranked
    .select("session_id", "id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([8, 15, 17]))
        .select("session_id", "id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "session_id",
        F.when(F.col("duration_sec_rounded") >= 73, "LARGE").otherwise("SMALL").alias("duration_sec_rounded_tier"),
        F.date_trunc("QUARTER", "event_time_day").alias("quarter_start"),
    )
    .agg(
        F.sum("duration_sec_rounded").alias("total_duration_sec_rounded"),
        F.count("*").alias("row_count"),
        F.percentile_approx("duration_sec_rounded", 0.9).alias("duration_sec_rounded_p90"),
    )
    .withColumn("duration_sec_rounded_p90_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("total_duration_sec_rounded").desc_nulls_first())
)
