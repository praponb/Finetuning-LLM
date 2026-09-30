from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service", "host").orderBy("log_ts", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .filter(F.col("status_code").between(290, 316))
    .select(
        "id",
        "host",
        "level",
        "k",
        "v",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

valid = (
    metrics
    .filter(F.col("row_seq") < 3)
    .select("*", F.when(F.col("level") == "DEBUG", 1).otherwise(0).alias("is_debug"))
)

filtered = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "k",
        "row_seq_2",
        "is_debug",
        F.when(F.col("k").isin(["trace", "version"]), "Priority").otherwise("Other").alias("k_group"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("is_debug") == 78)
    .withColumn("is_debug_band", F.when(F.col("is_debug") >= 82, "High").when(F.col("is_debug") >= 25, "Medium").otherwise("Low"))
)

result = (
    filtered_app_logs
    .groupBy(
        F.when(F.col("row_seq_2") >= 6, "HIGH").otherwise("LOW").alias("row_seq_2_tier"),
    )
    .agg(
        F.avg(F.when(F.col("k") == "trace", F.col("row_seq_2"))).alias("trace_avg_row_seq_2"),
        F.max(F.when(F.col("level") == "WARN", F.col("row_seq_2"))).alias("warn_max_row_seq_2"),
    )
    .orderBy(F.col("warn_max_row_seq_2").desc())
)
