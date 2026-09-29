from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("version"))))
    .withColumn("log_ts_hour", F.hour("log_ts"))
)

kept_keys = (
    cleaned
    .select("level", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("service") == "checkout")
        .select("level", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = verified.withColumn("host_min_log_ts_hour", F.min("log_ts_hour").over(w_hist))

base = (
    metrics
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("level").isin(["INFO", "ERROR"]))
    .select(
        "*",
        F.when(F.col("host").isin(["h3", "h2"]), "Priority").otherwise("Other").alias("host_group"),
        F.round("log_ts_hour", 2).alias("log_ts_hour_rounded"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

result = (
    base
    .groupBy("k")
    .pivot("level", ["ERROR", "DEBUG", "INFO"])
    .agg(F.max("log_ts_hour").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "k",
        F.col("ERROR_amount").alias("error_amount"),
        F.col("ERROR_n").alias("error_n"),
        F.col("DEBUG_amount").alias("debug_amount"),
        F.col("DEBUG_n").alias("debug_n"),
        F.col("INFO_amount").alias("info_amount"),
        F.col("INFO_n").alias("info_n"),
    )
)
