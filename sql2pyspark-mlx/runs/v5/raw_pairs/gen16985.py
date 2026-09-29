from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service").orderBy("status_code", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    parsed
    .filter(F.to_date("log_ts") >= "2025-07-01")
    .select(
        "id",
        "log_ts",
        "level",
        "duration_text",
        "has_trace",
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.row_number().over(w).alias("row_seq"),
    )
)
