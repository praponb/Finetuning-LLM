from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("status_code")
w_grp = Window.partitionBy("host").orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

valid = (
    exploded
    .filter(F.col("k").isNotNull() & (F.col("status_code") <= 401))
    .withColumn("status_code_rounded", F.round("status_code", 1))
    .withColumn("k_clean", F.coalesce("k", F.lit("NONE")))
)

cleaned = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01 12:00:00").cast("timestamp")) & F.hour("log_ts").between(11, 18))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "k",
        "v",
        F.when(F.col("status_code") >= 400, "High").when(F.col("status_code") >= 275, "Medium").otherwise("Low").alias("status_code_tier"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean_2"),
    )
)

scored = (
    cleaned
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("status_code_rank", F.rank().over(w_grp))
)

valid_app_logs = (
    scored
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "service",
        "host",
        "status_code",
        "k",
        "status_code_tier",
        "status_code_dense_rank",
        "status_code_rank",
        F.trim("host").alias("clean_host"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

grouped = (
    valid_app_logs
    .groupBy(
        F.when(F.col("status_code") >= 396, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code_dense_rank", 0.95).alias("status_code_dense_rank_p95"),
        F.round(F.avg("status_code_dense_rank"), 2).alias("avg_status_code_dense_rank"),
    )
)

result = (
    grouped
    .select("status_code_tier", "status_code_dense_rank_p95")
    .orderBy("status_code_dense_rank_p95")
)
