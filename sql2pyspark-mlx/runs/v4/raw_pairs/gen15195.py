from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy(F.col("duration_sec").desc())
w4 = Window.orderBy(F.col("duration_sec").asc_nulls_first())

matched_keys = (
    df_web_events
    .select("page", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") == 133)
        .select("page", "event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-10-18")))
    .withColumn("page_upper", F.upper("page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-09-29"))) & F.col("page").isNotNull())
    .select(
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "page_upper",
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
    )
)

metrics = (
    base
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w))
    .withColumn("duration_sec_count_30d", F.count("duration_sec").over(w2))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w3))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w4))
)

result = (
    metrics
    .select(
        "session_id",
        "page",
        "duration_sec",
        "page_upper",
        "event_time_period",
        "duration_sec_dense_rank",
        "duration_sec_cume_dist",
        "duration_sec_percent_rank",
    )
    .orderBy(F.col("duration_sec_percent_rank").asc_nulls_last())
)
