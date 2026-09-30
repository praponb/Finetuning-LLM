from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("level").orderBy("status_code", "id", "k")
w3 = Window.partitionBy("service").orderBy("latency_ms", "id", "k")
w4 = Window.partitionBy("level", "host")
w5 = Window.partitionBy("service").orderBy("prev_latency_ms", "id", "k").rowsBetween(-6, -1)
w6 = Window.partitionBy("host", "level")
w7 = Window.partitionBy("k").orderBy(F.col("prev_latency_ms").desc(), "id", "k")
w8 = Window.partitionBy("host").orderBy(F.col("status_code").desc(), "id", "k")
w9 = Window.partitionBy("level").orderBy("latest_rank", "id", "k")
w_grp = Window.partitionBy("k", "service").orderBy("prev_latency_ms_share", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("next_status_code", "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.lag("latency_ms", 2).over(w2).alias("prev_latency_ms"),
    )
)

metrics = (
    with_windows
    .withColumn("next_status_code", F.lead("status_code", 2).over(w3))
    .withColumn("prev_latency_ms_share", F.round((F.col("prev_latency_ms") / F.sum("prev_latency_ms").over(w4)), 4))
    .withColumn("corr_latency_ms_status_code_avg_6_prior", F.avg("corr_latency_ms_status_code").over(w5))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

metrics_app_logs = (
    deduped
    .select(
        "*",
        F.row_number().over(w9).alias("event_seq"),
        F.sum("corr_latency_ms_status_code").over(w_grp).alias("running_corr_latency_ms_status_code"),
        F.lead("corr_latency_ms_status_code_avg_6_prior", 1, 0).over(w11).alias("next_corr_latency_ms_status_code_avg_6_prior"),
    )
)

result = (
    metrics_app_logs
    .groupBy("level")
    .agg(
        F.avg(F.when(F.col("level") == "WARN", F.col("prev_latency_ms"))).alias("warn_avg_prev_latency_ms"),
        F.avg(F.when(F.col("host") == "h4", F.col("corr_latency_ms_status_code"))).alias("h4_avg_corr_latency_ms_status_code"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
    .filter(F.col("h4_avg_corr_latency_ms_status_code") > 0)
    .orderBy(F.col("status_code_bit_and").asc_nulls_last())
)
