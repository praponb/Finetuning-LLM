from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("corr_status_code_latency_ms").desc_nulls_last())
w_running = Window.partitionBy("service").orderBy(F.col("corr_status_code_latency_ms").asc_nulls_last())
w4 = Window.partitionBy("service").orderBy(F.col("corr_status_code_latency_ms").desc_nulls_last())
w5 = Window.partitionBy("service").orderBy(F.col("service_avg_corr_status_code_latency_ms").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "service",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
    )
)

scored = (
    metrics
    .withColumn("service_avg_corr_status_code_latency_ms", F.avg("corr_status_code_latency_ms").over(w))
    .withColumn("service_min_corr_status_code_latency_ms", F.min("corr_status_code_latency_ms").over(w))
    .withColumn("corr_status_code_latency_ms_rank", F.rank().over(w2))
    .withColumn("corr_status_code_latency_ms_rank_2", F.rank().over(w_running))
)

enriched = (
    scored
    .select(
        "id",
        "service",
        "corr_status_code_latency_ms",
        "service_avg_corr_status_code_latency_ms",
        "service_min_corr_status_code_latency_ms",
        F.round((F.col("corr_status_code_latency_ms_rank_2") / F.sum("corr_status_code_latency_ms_rank_2").over(w)), 4).alias("corr_status_code_latency_ms_rank_2_share"),
        F.rank().over(w4).alias("corr_status_code_latency_ms_rank_3"),
        F.cume_dist().over(w5).alias("service_avg_corr_status_code_latency_ms_cume_dist"),
    )
)

summary = (
    enriched
    .withColumn("service_min_corr_status_code_latency_ms_tier", F.when(F.col("service_min_corr_status_code_latency_ms") >= 1, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["service", "service_min_corr_status_code_latency_ms_tier"], ["service"], []],
        "service",
        "service_min_corr_status_code_latency_ms_tier",
    )
    .agg(
        F.max(F.when(F.col("service") == "auth", F.col("corr_status_code_latency_ms_rank_3"))).alias("auth_max_corr_status_code_latency_ms_rank_3"),
        F.round(F.avg("service_avg_corr_status_code_latency_ms"), 2).alias("avg_service_avg_corr_status_code_latency_ms"),
        F.max(F.when(F.col("service") == "billing", F.col("service_avg_corr_status_code_latency_ms"))).alias("billing_max_service_avg_corr_status_code_latency_ms"),
    )
)

result = (
    summary
    .select(
        "service",
        "service_min_corr_status_code_latency_ms_tier",
        "auth_max_corr_status_code_latency_ms_rank_3",
        "avg_service_avg_corr_status_code_latency_ms",
        "billing_max_service_avg_corr_status_code_latency_ms",
        F.when(F.col("service").rlike("^(checkout|billing)$"), "GROUP_A").when(F.col("service").rlike("^(auth|search)$"), "GROUP_B").otherwise("PRIORITY").alias("service_zone"),
    )
    .orderBy(F.col("auth_max_corr_status_code_latency_ms_rank_3").desc_nulls_first())
)
