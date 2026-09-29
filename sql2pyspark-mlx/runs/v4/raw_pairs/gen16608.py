from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("key")
w_recent = Window.partitionBy("service")
w4 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(-6, -1)
w5 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("key", "service").orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

ranked = (
    flattened
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    ranked
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w2))
    .withColumn("corr_latency_ms_status_code_2", F.corr("latency_ms", "status_code").over(w_recent))
    .withColumn("latency_ms_avg_6_prior", F.avg("latency_ms").over(w4))
)

kept_keys = (
    metrics
    .select("id", "service")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h4")
        .select("id", "service")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("corr_latency_ms_status_code_2_avg_30m", F.avg("corr_latency_ms_status_code_2").over(w5))
    .withColumn("corr_latency_ms_status_code_3", F.corr("latency_ms", "status_code").over(w_recent))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w6))
)

result = (
    enriched
    .groupBy(
        "key",
        F.when(F.col("corr_latency_ms_status_code_2_avg_30m") >= 0, "LARGE").otherwise("SMALL").alias("corr_latency_ms_status_code_2_avg_30m_tier"),
    )
    .agg(
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.count("*").alias("event_count"),
        F.round(F.avg("corr_latency_ms_status_code_2_avg_30m"), 2).alias("avg_corr_latency_ms_status_code_2_avg_30m"),
    )
    .orderBy(F.col("latency_ms_stddev").asc_nulls_first())
)
