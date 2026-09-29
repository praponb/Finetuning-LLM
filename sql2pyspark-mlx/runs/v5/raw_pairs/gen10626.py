from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "k")
w2 = Window.orderBy(F.col("status_code").desc(), "id", "k")
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level", "k").orderBy(F.col("log_ts").desc(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

scored = (
    lines
    .filter(F.col("status_code") > 271)
    .select(
        "*",
        F.lead("latency_ms", 1).over(w).alias("next_latency_ms"),
        F.ntile(4).over(w2).alias("status_code_bucket"),
    )
)

flagged = (
    scored
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w3) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "status_code_bucket",
        F.sum("session_start_flag").over(w4).alias("burst_id"),
    )
)

deduped = (
    sessionized
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    deduped
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.countDistinct("k").alias("distinct_ks"),
        F.countDistinct("k").alias("distinct_ks_2"),
    )
)

result = (
    totals
    .select("activity_day", "row_count", "num_levels", "distinct_ks", "distinct_ks_2")
    .orderBy(F.col("activity_day").desc(), F.col("num_levels").desc())
    .limit(5)
)
