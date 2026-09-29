from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("latency_ms_count_1h_sum_1h_tier").orderBy(F.col("span_minutes").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    lines
    .filter(F.col("latency_ms") != 1548)
    .select(
        "id",
        "log_ts",
        F.count("latency_ms").over(w_recent).alias("latency_ms_count_2h"),
        F.max("latency_ms").over(w_recent).alias("latency_ms_max_2h"),
        F.count("latency_ms").over(w2).alias("latency_ms_count_1h"),
    )
)

cleaned = (
    scored
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & (F.to_date("log_ts") >= "2025-07-01"))
    .select(
        "*",
        F.coalesce("latency_ms_count_1h", F.lit(0)).alias("latency_ms_count_1h_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("latency_ms_count_1h").over(w2).alias("latency_ms_count_1h_sum_1h"),
        F.sum("latency_ms_count_1h").over(w_recent).alias("latency_ms_count_1h_sum_2h"),
        F.avg("latency_ms_count_2h").over(w3).alias("latency_ms_count_2h_avg_30m"),
        F.count("latency_ms_count_2h").over(w_recent).alias("latency_ms_count_2h_count_2h"),
        F.max("latency_ms_max_2h").over(w3).alias("latency_ms_max_2h_max_30m"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("latency_ms_count_1h_sum_1h") >= 88, "HIGH").otherwise("LOW").alias("latency_ms_count_1h_sum_1h_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("log_ts").alias("first_seen"),
        F.countDistinct("id").alias("distinct_ids_2"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .withColumn("span_minutes_percent_rank", F.percent_rank().over(w4))
)

result = (
    grouped
    .select(
        "latency_ms_count_1h_sum_1h_tier",
        "hour_window",
        "distinct_ids",
        "first_seen",
        "distinct_ids_2",
        "span_minutes",
        "span_minutes_percent_rank",
    )
    .orderBy(F.col("hour_window").desc_nulls_last())
)
