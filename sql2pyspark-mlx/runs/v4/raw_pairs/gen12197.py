from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("log_ts_hour").desc_nulls_first())
w2 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

filtered = (
    exploded
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("attr_key").isNotNull() & (F.col("level") == "INFO"))
    .withColumn("level_group", F.when(F.col("level").isin(["DEBUG", "INFO"]), "Core").otherwise("Other"))
)

base = (
    filtered
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("attr_key").isNotNull())
    .withColumn("host_upper", F.upper("host"))
    .withColumn("level_lower", F.lower("level"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("OTHER")))
)

valid = (
    base
    .filter((F.col("log_ts") > F.lit("2025-06-01 07:00:00").cast("timestamp")) & F.col("level").isin(["DEBUG", "ERROR", "INFO"]))
    .select(
        "*",
        F.hour("log_ts").alias("log_ts_hour"),
        F.upper("attr_key").alias("attr_key_upper"),
    )
)

valid_app_logs = (
    valid
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull() & (F.col("attr_key") == "version"))
    .select(
        "service",
        "host",
        "attr_key",
        "v",
        "level_group",
        "attr_key_filled",
        "log_ts_hour",
        "attr_key_upper",
        F.when(F.col("level") == "ERROR", 1).otherwise(0).alias("is_error"),
    )
)

remaining_keys = (
    valid_app_logs
    .select("host", "service")
    .intersect(
        df_app_logs
        .filter(F.to_date("log_ts") < "2023-03-01")
        .select("host", "service")
    )
)

verified = (
    valid_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("log_ts_hour_cume_dist", F.cume_dist().over(w))
    .withColumn("service_max_log_ts_hour", F.max("log_ts_hour").over(w2))
)

result = (
    enriched
    .select(
        "v",
        "level_group",
        "attr_key_filled",
        "attr_key_upper",
        "is_error",
        "log_ts_hour_cume_dist",
        "service_max_log_ts_hour",
    )
    .orderBy(F.col("log_ts_hour_cume_dist").desc())
)
