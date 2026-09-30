from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("num_context").desc(), "id")
w2 = Window.partitionBy("service").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.size("context").alias("num_context"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("num_context", ignorenulls=True).over(w2).alias("last_known_num_context"),
    )
)
