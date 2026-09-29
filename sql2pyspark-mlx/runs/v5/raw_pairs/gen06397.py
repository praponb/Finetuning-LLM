from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code_bit_or").desc())

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .groupBy(
        "service",
        F.when(F.col("status_code") >= 460, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
    .withColumn("status_code_bit_or_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("debug_events").desc())
)
