from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("num_context").desc(), "id")
w2 = Window.partitionBy("host").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .filter(F.hour("log_ts").between(7, 15))
    .select(
        "id",
        "host",
        "latency_ms",
        "has_region",
        "duration_text",
        F.row_number().over(w).alias("event_seq"),
        F.sum("num_context").over(w2).alias("running_num_context"),
    )
)
