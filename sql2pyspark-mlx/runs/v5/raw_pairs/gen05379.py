from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("key", "host").orderBy("status_code", "id", "key")
w4 = Window.partitionBy("host")
w5 = Window.orderBy(F.col("latency_ms").desc(), "id", "key")
w6 = Window.partitionBy("key", "host").orderBy(F.col("status_code_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("service", "host").orderBy(F.col("log_ts").desc(), "id", "key")
w_grp = Window.partitionBy("host").orderBy("prev_latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("key", "service").orderBy(F.col("status_code_cume_dist").desc(), "id", "key")
w10 = Window.partitionBy("key").orderBy(F.col("status_code_cume_dist").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

metrics = (
    exploded
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w2))
    .withColumn("prev_latency_ms", F.lag("latency_ms", 1, 0).over(w3))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4))
    .withColumn("latency_ms_bucket", F.ntile(10).over(w5))
)

scored = (
    metrics
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "corr_latency_ms_status_code",
        "status_code_cume_dist",
        "prev_latency_ms",
        "latency_ms_bucket",
        F.rank().over(w6).alias("status_code_cume_dist_rank"),
        F.row_number().over(w7).alias("seq_no"),
        F.sum("status_code_share").over(w_grp).alias("cumulative_status_code_share"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

report = (
    deduped
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "status_code_cume_dist",
        "latency_ms_bucket",
        "status_code_cume_dist_rank",
        "seq_no",
        "cumulative_status_code_share",
        F.when(F.col("key").rlike("^(region|version)$"), "CORE").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("status_code_cume_dist_rank").desc_nulls_first())
)

result = (
    report
    .select(
        "id",
        "latency_ms",
        "status_code_cume_dist",
        "latency_ms_bucket",
        "status_code_cume_dist_rank",
        "seq_no",
        "cumulative_status_code_share",
        "key_zone",
        F.when(F.col("key_zone").rlike("^(GROUP_B|CORE)$"), "OTHER_ZONE").when(F.col("key_zone").rlike("^(EXTENDED)$"), "CORE").otherwise("GROUP_A").alias("key_zone_zone"),
    )
    .orderBy(
        F.col("latency_ms_bucket").desc_nulls_last(),
        F.col("status_code_cume_dist").desc(),
    )
)
