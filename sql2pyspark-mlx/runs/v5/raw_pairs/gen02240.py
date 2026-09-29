from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "level").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "service")
w5 = Window.orderBy("status_code", "id", "attr_key")
w6 = Window.partitionBy("level").orderBy(F.col("status_code").desc())
w7 = Window.partitionBy("attr_key").orderBy("level_stddev_samp_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("attr_key").orderBy("prev_status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

ranked = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    ranked
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.sum("status_code").over(w3).alias("running_status_code"),
        F.stddev_samp("latency_ms").over(w4).alias("level_stddev_samp_latency_ms"),
        F.lag("status_code", 1).over(w5).alias("prev_status_code"),
        F.rank().over(w6).alias("status_code_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("running_avg_running_status_code", F.avg("running_status_code").over(w7))
    .withColumn("running_count_running_status_code", F.count("running_status_code").over(w7))
    .withColumn("running_sum_running_status_code", F.sum("running_status_code").over(w7))
    .withColumn("running_status_code_rank", F.sum("status_code_rank").over(w8))
)

cleaned = (
    enriched
    .filter(F.col("running_sum_running_status_code").isNull())
    .withColumn("running_avg_running_status_code_filled", F.coalesce("running_avg_running_status_code", F.lit(0)))
)

result = (
    cleaned
    .groupBy(
        "attr_key",
        F.when(F.col("running_sum_running_status_code") >= 10500, "HIGH").otherwise("LOW").alias("running_sum_running_status_code_tier"),
    )
    .agg(
        F.max("level_stddev_samp_latency_ms").alias("max_level_stddev_samp_latency_ms"),
        F.percentile_approx("level_stddev_samp_latency_ms", 0.95).alias("level_stddev_samp_latency_ms_p95"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count_if(F.col("attr_key") == "trace").alias("trace_events"),
        F.sum(F.when(F.col("attr_key") == "version", F.col("running_avg_running_status_code"))).alias("version_total_running_avg_running_status_code"),
    )
    .filter(F.col("distinct_attr_keys") < 1000)
)
