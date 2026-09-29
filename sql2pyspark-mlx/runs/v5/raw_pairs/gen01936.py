from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("retry_count_tier").orderBy(F.col("retry_count_p50").desc())

flattened_attrs = (
    df_app_logs
    .filter(F.to_date("log_ts") < "2024-06-01")
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("retry_count") >= 4, "PREMIUM").otherwise("STANDARD").alias("retry_count_tier"),
        F.when(F.col("status_code") >= 398, "HIGH").otherwise("LOW").alias("status_code_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("retry_count", 0.5).alias("retry_count_p50"),
    )
    .withColumn("retry_count_p50_percent_rank", F.percent_rank().over(w))
)
