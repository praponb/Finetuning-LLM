from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("status_code_rank").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w5 = Window.partitionBy("service").orderBy("status_code_rank", "id", "attr_key").rowsBetween(-6, -1)
w6 = Window.partitionBy("service").orderBy("cumulative_status_code", "id", "attr_key")
w7 = Window.partitionBy("attr_key").orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

filtered = (
    lines
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .select("*", F.concat_ws("/", "level", "service").alias("level_service_key"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.rank().over(w_running).alias("status_code_rank"),
        F.sum("status_code").over(w2).alias("cumulative_status_code"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.avg("status_code_rank").over(w5).alias("status_code_rank_avg_6_prior"),
        F.row_number().over(w6).alias("row_seq"),
        F.rank().over(w7).alias("status_code_rank_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "service",
        "log_ts",
        "attr_key",
        "v",
        "level_service_key",
        "status_code_rank",
        "cumulative_status_code",
        "row_seq",
    )
    .orderBy(F.col("status_code_rank").asc_nulls_first())
)
