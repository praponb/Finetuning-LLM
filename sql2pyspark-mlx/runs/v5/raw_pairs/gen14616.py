from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k", "host")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

remaining_keys = (
    exploded
    .select("host", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("latency_ms") <= 2325)
        .select("host", "id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("k").isNotNull())
    .withColumn("host_lower", F.lower("host"))
    .withColumn("k_clean", F.coalesce("k", F.lit("NONE")))
)

metrics = (
    filtered
    .filter(F.col("status_code") > 404)
    .select(
        "id",
        "v",
        "host_lower",
        "k_clean",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code_2"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.avg("latency_ms").over(w3).alias("k_avg_latency_ms"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code_3"),
    )
)

valid = (
    metrics
    .filter(F.col("corr_latency_ms_status_code_2").between(0.0, 0.2))
    .select(
        "id",
        "v",
        "k_clean",
        "corr_latency_ms_status_code",
        "corr_latency_ms_status_code_2",
        "corr_status_code_latency_ms",
        "k_avg_latency_ms",
        "corr_latency_ms_status_code_3",
        F.col("corr_status_code_latency_ms").cast("int").alias("corr_status_code_latency_ms_int"),
        F.greatest("corr_latency_ms_status_code", "corr_latency_ms_status_code_2").alias("greatest_corr_latency_ms_status_code_corr_latency_ms_status_code_2"),
    )
)

base = (
    valid
    .filter(F.col("k_avg_latency_ms") >= 1852)
    .select(
        "*",
        F.when(F.col("corr_status_code_latency_ms_int") >= 83, "High").when(F.col("corr_status_code_latency_ms_int") >= 36, "Medium").otherwise("Low").alias("corr_status_code_latency_ms_int_band"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("corr_latency_ms_status_code_2") >= 0.7, "HIGH").otherwise("LOW").alias("corr_latency_ms_status_code_2_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("greatest_corr_latency_ms_status_code_corr_latency_ms_status_code_2").alias("avg_greatest_corr_latency_ms_status_code_corr_latency_ms_status_code_2"),
        F.percentile_approx("corr_latency_ms_status_code", 0.95).alias("corr_latency_ms_status_code_p95"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
