from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_context", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("tier", "level").orderBy(F.col("latency_ms").desc(), "id")
w3 = Window.partitionBy("tier").orderBy(F.col("status_code_high").desc(), "id")
w4 = Window.partitionBy("tier", "level")
w5 = Window.partitionBy("level")

parsed = (
    df_app_logs
    .filter(F.col("level").isin(["WARN", "DEBUG", "INFO"]))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .filter(F.col("log_ts") < "2025-10-01")
    .select(
        "id",
        "status_code",
        "status_code_high",
        "num_context",
        "tier",
        F.avg("status_code").over(w).alias("status_code_avg_3_prior"),
        F.ntile(4).over(w2).alias("latency_ms_bucket"),
        F.ntile(5).over(w3).alias("status_code_high_bucket"),
        F.avg("latency_ms").over(w4).alias("tier_avg_latency_ms"),
        F.corr("status_code", "latency_ms").over(w5).alias("corr_status_code_latency_ms"),
    )
)
