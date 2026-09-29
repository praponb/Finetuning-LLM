from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "key")
w2 = Window.partitionBy("key", "service").orderBy(F.col("service_count_status_code").asc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("service_count_status_code").desc_nulls_last())
w4 = Window.partitionBy("service").orderBy(F.col("service_count_status_code_percent_rank").desc_nulls_first())
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("service").orderBy(F.col("least_service_count_status_code_percent_rank_service_count_status_code").desc_nulls_last())
w7 = Window.partitionBy("service").orderBy(F.col("service_count_status_code_rank").desc_nulls_last())
w8 = Window.orderBy(F.col("service_count_status_code_rank").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

enriched = (
    lines
    .select(
        "id",
        "service",
        "status_code",
        "key",
        F.count("status_code").over(w).alias("service_count_status_code"),
    )
)

enriched_app_logs = (
    enriched
    .filter(F.col("service") == "search")
    .withColumn("service_count_status_code_percent_rank", F.percent_rank().over(w2))
    .withColumn("service_count_status_code_rank", F.rank().over(w3))
)

cleaned = (
    enriched_app_logs
    .filter(F.col("key").isNotNull())
    .withColumn("least_service_count_status_code_percent_rank_service_count_status_code", F.least("service_count_status_code_percent_rank", "service_count_status_code"))
    .withColumn("key_clean", F.coalesce("key", F.lit("UNKNOWN")))
)

filtered = (
    cleaned
    .filter(F.col("service") == "auth")
    .select(
        "id",
        "service",
        "key",
        "service_count_status_code",
        "service_count_status_code_percent_rank",
        "service_count_status_code_rank",
        "least_service_count_status_code_percent_rank_service_count_status_code",
        "key_clean",
        F.upper("key").alias("key_upper"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean_2"),
    )
)

cleaned_app_logs = (
    filtered
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .withColumn("service_lower", F.lower("service"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("N/A")))
)

enriched_app_logs_2 = (
    cleaned_app_logs
    .select(
        "service",
        "service_count_status_code",
        "service_count_status_code_rank",
        "least_service_count_status_code_percent_rank_service_count_status_code",
        "key_clean",
        "service_lower",
        "resolved_key",
        F.dense_rank().over(w4).alias("service_count_status_code_percent_rank_dense_rank"),
        F.min("service_count_status_code_rank").over(w5).alias("service_min_service_count_status_code_rank"),
        F.cume_dist().over(w6).alias("least_service_count_status_code_percent_rank_service_count_status_code_cume_dist"),
        F.percent_rank().over(w7).alias("service_count_status_code_rank_percent_rank"),
        F.dense_rank().over(w8).alias("service_count_status_code_rank_dense_rank"),
    )
)

result = (
    enriched_app_logs_2
    .select(
        "service_count_status_code",
        "service_count_status_code_rank",
        "least_service_count_status_code_percent_rank_service_count_status_code",
        "key_clean",
        "service_lower",
        "resolved_key",
        "service_min_service_count_status_code_rank",
        "least_service_count_status_code_percent_rank_service_count_status_code_cume_dist",
        "service_count_status_code_rank_dense_rank",
    )
    .orderBy(F.col("service_min_service_count_status_code_rank").asc_nulls_first())
)
