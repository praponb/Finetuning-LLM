from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["trace"].alias("trace"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.first("version_or_default", ignorenulls=True).over(w).alias("first_known_version_or_default"),
        F.first("version_or_default", ignorenulls=True).over(w2).alias("first_known_version_or_default_2"),
    )
)
