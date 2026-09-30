from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "log_ts",
        "k",
        "value",
        F.date_format("log_ts", "yyyy-MM-dd").alias("log_ts_label"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

filtered = (
    cleaned
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull() & F.hour("log_ts").between(12, 18))
    .select(
        "log_ts",
        "k",
        "value",
        "log_ts_label",
        "k_clean",
        F.when(F.col("k").isin(["trace", "region"]), "Core").otherwise("Other").alias("k_group"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("log_ts") >= F.lit("2025-06-02 01:00:00").cast("timestamp"))
    .select("*", F.length("k").alias("k_length"))
)

filtered_app_logs_2 = (
    filtered_app_logs
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("log_ts_hour", F.hour("log_ts"))
)

result = (
    filtered_app_logs_2
    .select("log_ts", "value", "log_ts_label", "k_clean", "k_group", "k_length")
    .orderBy("log_ts", F.col("k_length").desc())
)
