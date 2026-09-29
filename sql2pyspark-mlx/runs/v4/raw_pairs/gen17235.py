from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("host").orderBy(F.col("status_code_count_30m").desc_nulls_first())
w4 = Window.partitionBy("attr_key", "host").orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("status_code_tier").orderBy(F.col("distinct_hosts").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

scored = (
    lines
    .withColumn("status_code_count_30m", F.count("status_code").over(w))
    .withColumn("status_code_max_30m", F.max("status_code").over(w))
    .withColumn("attr_key_count_latency_ms", F.count("latency_ms").over(w2))
)

with_windows = (
    scored
    .withColumn("status_code_count_30m_dense_rank", F.dense_rank().over(w3))
    .withColumn("status_code_rank", F.rank().over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_code") >= 403, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        F.when(F.col("status_code_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("status_code_rank_tier"),
    )
    .agg(
        F.countDistinct("host").alias("distinct_hosts"),
        F.min("log_ts").alias("started_at"),
        F.max("log_ts").alias("last_seen"),
    )
    .withColumn("distinct_hosts_dense_rank", F.dense_rank().over(w5))
)
