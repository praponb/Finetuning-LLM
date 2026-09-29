from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("k").orderBy(F.col("days_since_log_ts").asc_nulls_last())
w3 = Window.orderBy("days_since_log_ts")
w4 = Window.orderBy(F.col("days_since_log_ts").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

filtered = (
    exploded
    .filter(~F.col("k").isin(["region", "version"]))
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "k",
        "attr_value",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("k").isNotNull())
    .withColumn("host_title", F.initcap(F.trim("host")))
    .withColumn("resolved_k_2", F.coalesce("k", F.lit("DIRECT")))
)

scored = (
    filtered_app_logs
    .select(
        "service",
        "host",
        "status_code",
        "k",
        "days_since_log_ts",
        "resolved_k_2",
        F.stddev_samp("days_since_log_ts").over(w).alias("host_stddev_samp_days_since_log_ts"),
        F.dense_rank().over(w2).alias("days_since_log_ts_dense_rank"),
        F.percent_rank().over(w3).alias("days_since_log_ts_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("host") == "h3")
    .withColumn("days_since_log_ts_str", F.col("days_since_log_ts").cast("string"))
    .withColumn("host_group", F.when(F.col("host").isin(["h2", "h3"]), "Core").otherwise("Other"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("N/A")))
)

enriched = (
    base
    .withColumn("host_count_days_since_log_ts_dense_rank", F.count("days_since_log_ts_dense_rank").over(w))
    .withColumn("host_avg_days_since_log_ts_percent_rank", F.avg("days_since_log_ts_percent_rank").over(w))
    .withColumn("days_since_log_ts_cume_dist", F.cume_dist().over(w4))
)

result = (
    enriched
    .select(
        "service",
        "k",
        "days_since_log_ts",
        "days_since_log_ts_percent_rank",
        "days_since_log_ts_str",
        "host_group",
        "host_count_days_since_log_ts_dense_rank",
        "host_avg_days_since_log_ts_percent_rank",
        "days_since_log_ts_cume_dist",
        F.when(F.col("service").rlike("^(billing|search)$"), "OTHER_ZONE").otherwise("CORE").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").desc_nulls_last())
)
