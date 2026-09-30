from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("status_code", "id", "key")
w3 = Window.orderBy("next_status_code", "id", "key").rowsBetween(-3, -1)
w4 = Window.partitionBy("host").orderBy("row_seq", "id", "key")
w_running = Window.orderBy("next_status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

enriched = (
    flattened
    .filter(F.col("key") == "version")
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        "value",
        F.row_number().over(w).alias("row_seq"),
        F.lead("status_code", 1).over(w2).alias("next_status_code"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("next_status_code").over(w3).alias("next_status_code_avg_3_prior"),
        F.ntile(3).over(w4).alias("row_seq_bucket"),
        F.row_number().over(w_running).alias("row_seq_2"),
    )
)

valid = (
    scored
    .filter((F.col("log_ts") >= F.lit("2025-06-01 13:00:00").cast("timestamp")) & F.col("row_seq").between(5, 7))
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        "row_seq",
        "next_status_code",
        "next_status_code_avg_3_prior",
        "row_seq_bucket",
        F.round("row_seq_2", 0).alias("row_seq_2_rounded"),
        (F.col("next_status_code") + F.col("next_status_code_avg_3_prior")).alias("next_status_code_plus_next_status_code_avg_3_prior"),
    )
)

result = (
    valid
    .withColumn("next_status_code_plus_next_status_code_avg_3_prior_tier", F.when(F.col("next_status_code_plus_next_status_code_avg_3_prior") >= 81, "LARGE").otherwise("SMALL"))
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .groupingSets(
        [["next_status_code_plus_next_status_code_avg_3_prior_tier", "activity_day"], ["next_status_code_plus_next_status_code_avg_3_prior_tier"], []],
        "next_status_code_plus_next_status_code_avg_3_prior_tier",
        "activity_day",
    )
    .agg(
        F.percentile_approx("next_status_code_plus_next_status_code_avg_3_prior", 0.95).alias("next_status_code_plus_next_status_code_avg_3_prior_p95"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
        F.min("log_ts").alias("first_log_ts"),
        F.sum(F.when(F.col("host") == "h4", F.col("row_seq_bucket")).otherwise(0)).alias("h4_row_seq_bucket"),
        F.avg(F.when(F.col("key") == "region", F.col("row_seq_2_rounded"))).alias("region_avg_row_seq_2_rounded"),
    )
    .orderBy("event_count")
)
