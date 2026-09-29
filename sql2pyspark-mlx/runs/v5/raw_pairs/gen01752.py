from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc(), "id", "key")
w2 = Window.orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key", "host").orderBy("log_ts", "id", "key").rowsBetween(-2, -1)
w4 = Window.partitionBy("key").orderBy("running_status_code", "id", "key")
w5 = Window.partitionBy("key", "host").orderBy("running_status_code", "id", "key").rowsBetween(-3, -1)
w6 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

ranked = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "host", "log_ts", "status_code", "key", "value")
)

with_windows = (
    ranked
    .filter(F.col("host") == "h1")
    .withColumn("running_status_code", F.sum("status_code").over(w2))
    .withColumn("status_code_avg_2_prior", F.avg("status_code").over(w3))
)

enriched = (
    with_windows
    .select(
        "id",
        "host",
        "log_ts",
        "key",
        "value",
        "running_status_code",
        "status_code_avg_2_prior",
        F.ntile(5).over(w4).alias("running_status_code_bucket"),
        F.avg("status_code_avg_2_prior").over(w5).alias("status_code_avg_2_prior_avg_3_prior"),
        F.sum("status_code").over(w6).alias("status_code_sum_2h"),
    )
)

result = (
    enriched
    .select(
        "id",
        "log_ts",
        "key",
        "value",
        "running_status_code",
        "status_code_avg_2_prior",
        "status_code_avg_2_prior_avg_3_prior",
        "status_code_sum_2h",
    )
    .orderBy("status_code_avg_2_prior_avg_3_prior")
)
