from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("days_since_log_ts").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

valid = (
    lines
    .filter((F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("attr_key_upper", F.upper("attr_key"))
)

filtered = (
    valid
    .filter(F.col("attr_key").isNotNull() & (F.to_date("log_ts") >= "2025-10-05"))
    .withColumn("attr_key_lower", F.lower("attr_key"))
    .withColumn("log_ts_period", F.date_trunc("year", "log_ts"))
)

valid_app_logs = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "attr_key",
        "value",
        "attr_key_upper",
        "attr_key_lower",
        "log_ts_period",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

base = (
    valid_app_logs
    .filter(F.col("log_ts") > F.lit("2025-06-01 13:00:00").cast("timestamp"))
    .select(
        "id",
        "log_ts",
        "attr_key",
        "value",
        "attr_key_lower",
        "log_ts_period",
        "days_since_log_ts",
        F.when(F.col("attr_key").isin(["region", "trace"]), "Priority").otherwise("Other").alias("attr_key_group"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled"),
    )
)

with_windows = (
    base
    .select(
        "value",
        "attr_key_lower",
        "days_since_log_ts",
        "attr_key_group",
        "attr_key_filled",
        F.percent_rank().over(w).alias("days_since_log_ts_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "value",
        "attr_key_lower",
        "days_since_log_ts",
        "attr_key_group",
        "attr_key_filled",
    )
    .orderBy(F.col("days_since_log_ts").desc_nulls_last())
)
