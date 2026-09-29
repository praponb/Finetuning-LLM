from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w_recent = Window.partitionBy("level").orderBy("status_code")
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy("log_ts", "id", "k").rowsBetween(-6, -1)
w5 = Window.partitionBy("host", "k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("k", "host").orderBy(F.col("running_status_code").desc_nulls_first())
w8 = Window.partitionBy("host", "level").orderBy("status_code_rank_avg_6_prior", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("host").orderBy(F.col("first_known_status_code_rank").desc(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

deduped = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.rank().over(w_recent).alias("status_code_rank"),
        F.max("status_code").over(w_hist).alias("status_code_max_15m"),
    )
)

metrics = (
    with_windows
    .filter(F.col("level") == "DEBUG")
    .select(
        "*",
        F.avg("status_code_rank").over(w4).alias("status_code_rank_avg_6_prior"),
        F.sum("status_code").over(w5).alias("running_status_code"),
        F.count("status_code_max_15m").over(w6).alias("status_code_max_15m_count_30m"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "k",
        "value",
        "dedup_rank",
        "status_code_max_15m",
        "running_status_code",
        F.dense_rank().over(w7).alias("running_status_code_dense_rank"),
        F.first("status_code_rank", ignorenulls=True).over(w8).alias("first_known_status_code_rank"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank_2", F.row_number().over(w9))
    .filter(F.col("dedup_rank_2") == 1)
)

by_type = (
    latest
    .groupBy("level", "k")
    .pivot("host", ["h3", "h4", "h2"])
    .agg(F.sum("first_known_status_code_rank").alias("amount"))
)

result = (
    by_type
    .select("level", "k", "h3", "h2")
    .orderBy(F.col("h2").desc_nulls_first())
)
