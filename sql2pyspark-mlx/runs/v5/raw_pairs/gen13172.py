from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts_hour").asc_nulls_first())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("host_lower", F.lower("host"))
)

cleaned = (
    valid
    .filter(F.col("host") == "h4")
    .select("*", F.hour("log_ts").alias("log_ts_hour"))
)

base = (
    cleaned
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 15:00:00"))) & F.col("log_ts_hour").between(49, 65))
    .select(
        "log_ts",
        "attr_value",
        "log_ts_hour",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
    )
)

cleaned_app_logs = (
    base
    .filter((F.col("log_ts") > F.lit("2025-06-01 13:00:00").cast("timestamp")) & F.col("log_ts_hour").between(21, 67))
    .withColumn("log_ts_date", F.to_date("log_ts"))
)

base_app_logs = (
    cleaned_app_logs
    .filter(F.col("log_ts") >= F.lit("2025-06-01 15:00:00").cast("timestamp"))
    .withColumn("log_ts_hour_str", F.col("log_ts_hour").cast("string"))
)

enriched = (
    base_app_logs
    .select(
        "log_ts",
        "attr_value",
        "log_ts_hour",
        "log_ts_date",
        F.cume_dist().over(w).alias("log_ts_hour_cume_dist"),
        F.max("log_ts_hour").over(w2).alias("log_ts_hour_max_30m"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("log_ts_hour_max_30m") >= 85, "HIGH").otherwise("LOW").alias("log_ts_hour_max_30m_tier"),
        F.when(F.col("log_ts_hour_cume_dist") >= 0.84, "LARGE").otherwise("SMALL").alias("log_ts_hour_cume_dist_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.min("log_ts").alias("started_at"),
        F.min("log_ts").alias("started_at_2"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .orderBy(F.col("log_ts_hour_max_30m_tier").desc())
)
