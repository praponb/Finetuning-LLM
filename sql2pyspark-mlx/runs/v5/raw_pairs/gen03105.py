from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")
w_hist = Window.partitionBy("level")
w3 = Window.partitionBy("host")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

base = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01 13:00:00").cast("timestamp")) & (F.col("status_code") <= 386))
    .select(
        "*",
        F.length("host").alias("host_length"),
        F.initcap(F.trim("level")).alias("level_title"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

base_app_logs = (
    base
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 12:00:00"))) & ~F.col("level").isin(["INFO", "WARN", "ERROR"]))
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "attr_key",
        "value",
        "host_length",
        "level_title",
        "resolved_attr_key",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

filtered = (
    base_app_logs
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("version"))))
    .withColumn("host_length_2", F.length("host"))
)

cleaned = (
    filtered
    .filter(F.col("days_since_log_ts") < 42)
    .withColumn("host_lower", F.lower("host"))
    .withColumn("resolved_attr_key_2", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

filtered_app_logs = (
    cleaned
    .filter(F.col("status_code") == 350)
    .select(
        "*",
        F.least("status_code", "host_length").alias("least_status_code_host_length"),
    )
)

enriched = (
    filtered_app_logs
    .filter(F.col("status_code").between(369, 430))
    .select(
        "id",
        "host",
        "status_code",
        "value",
        "level_title",
        "resolved_attr_key",
        "host_length_2",
        "host_lower",
        "least_status_code_host_length",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.count("status_code").over(w_hist).alias("level_count_status_code"),
        F.stddev_samp("host_length").over(w3).alias("host_stddev_samp_host_length"),
    )
)

stats = (
    enriched
    .cube(
        F.when(F.col("least_status_code_host_length") >= 77, "PREMIUM").otherwise("STANDARD").alias("least_status_code_host_length_tier"),
        "host",
    )
    .agg(
        F.sum(F.when(F.col("host") == "h2", F.col("status_code")).otherwise(0)).alias("h2_status_code"),
        F.avg("host_stddev_samp_host_length").alias("avg_host_stddev_samp_host_length"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
    )
)

result = (
    stats
    .select("avg_host_stddev_samp_host_length", "h1_rows")
    .orderBy(F.col("avg_host_stddev_samp_host_length").asc_nulls_last())
)
