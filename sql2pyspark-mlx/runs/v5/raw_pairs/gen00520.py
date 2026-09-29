from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code") < 266)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.size("context").alias("num_context"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "latency_ms",
        "status_code",
        "num_context",
        "version_or_default",
        F.lag("num_context").over(w).alias("prev_num_context"),
        F.lead("num_context").over(w).alias("next_num_context"),
        F.lead("latency_ms").over(w).alias("next_latency_ms"),
        F.row_number().over(w).alias("seq_no"),
        F.lag("retry_count", 2).over(w).alias("prev_retry_count"),
    )
)
