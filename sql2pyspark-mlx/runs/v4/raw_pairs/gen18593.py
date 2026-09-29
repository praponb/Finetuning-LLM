from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_ordered = Window.orderBy("status_code")
w5 = Window.partitionBy("service", "attr_key")
w6 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.partitionBy("service").orderBy(F.col("status_code_dense_rank").desc())
w8 = Window.partitionBy("attr_key", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

metrics = (
    exploded
    .withColumn("status_code_avg_15m", F.avg("status_code").over(w))
    .withColumn("status_code_rank", F.rank().over(w2))
    .withColumn("status_code_max_30m", F.max("status_code").over(w3))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("service_count_status_code", F.count("status_code").over(w5))
)

enriched = (
    metrics
    .withColumn("status_code_avg_15m_max_1h", F.max("status_code_avg_15m").over(w6))
    .withColumn("status_code_dense_rank_dense_rank", F.dense_rank().over(w7))
    .withColumn("service_count_status_code_sum_15m", F.sum("service_count_status_code").over(w8))
)

cleaned = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("attr_key").isNotNull() & (F.col("service_count_status_code_sum_15m") < 75))
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "attr_key",
        "v",
        "status_code_avg_15m",
        "status_code_rank",
        "status_code_max_30m",
        "status_code_avg_15m_max_1h",
        "status_code_dense_rank_dense_rank",
        "service_count_status_code_sum_15m",
        F.round((F.col("service_count_status_code_sum_15m") * 1.1), 2).alias("adjusted_service_count_status_code_sum_15m"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

stats = (
    cleaned
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        "service",
        F.when(F.col("status_code") >= 425, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("status_code_max_30m")).otherwise(0)).alias("checkout_status_code_max_30m"),
        F.max("status_code").alias("max_status_code"),
        F.avg("service_count_status_code_sum_15m").alias("avg_service_count_status_code_sum_15m"),
        F.max("log_ts").alias("last_log_ts"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .filter(F.col("checkout_status_code_max_30m") >= 0)
)

result = (
    stats
    .select(
        "hour_window",
        "service",
        "status_code_tier",
        "checkout_status_code_max_30m",
        "max_status_code",
        "last_log_ts",
        "service_list",
    )
    .orderBy("service")
)
