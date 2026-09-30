from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy(F.col("status_code_high").asc_nulls_first())
w3 = Window.partitionBy("attr_key", "host")
w_grp = Window.partitionBy("host")
w5 = Window.partitionBy("host").orderBy(F.col("has_fault").asc_nulls_first())
w_grp_2 = Window.orderBy(F.col("status_code_high_cume_dist").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "host",
        "status_code",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .withColumn("attr_key_min_has_fault", F.min("has_fault").over(w))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w2))
    .withColumn("attr_key_sum_has_fault", F.sum("has_fault").over(w3))
)

enriched = (
    metrics
    .withColumn("host_min_attr_key_sum_has_fault", F.min("attr_key_sum_has_fault").over(w_grp))
    .withColumn("has_fault_rank", F.rank().over(w5))
    .withColumn("status_code_high_cume_dist_dense_rank", F.dense_rank().over(w_grp_2))
)

result = (
    enriched
    .groupBy(
        "attr_key",
        F.when(F.col("status_code") >= 419, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.when(F.col("attr_key_min_has_fault") >= 1, "PREMIUM").otherwise("STANDARD").alias("attr_key_min_has_fault_tier"),
    )
    .agg(
        F.percentile_approx("has_fault", 0.95).alias("has_fault_p95"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.avg("attr_key_sum_has_fault").alias("avg_attr_key_sum_has_fault"),
        F.percentile_approx("attr_key_min_has_fault", [0.25, 0.5, 0.75]).alias("attr_key_min_has_fault_quartiles"),
    )
    .orderBy(F.col("attr_key").desc())
)
