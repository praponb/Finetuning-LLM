from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "level").orderBy("latency_ms", "id")
w3 = Window.partitionBy("level", "host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context")["region"].alias("region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "level",
        "latency_ms",
        "has_trace",
        F.sum("retry_count").over(w).alias("running_retry_count"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("status_code_high").over(w3).alias("cumulative_status_code_high"),
    )
)
