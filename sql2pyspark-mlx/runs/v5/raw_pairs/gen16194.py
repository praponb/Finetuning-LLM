from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.size("context").alias("num_context"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    flattened_attrs
    .withColumn("next_num_context", F.lead("num_context").over(w))
    .withColumn("prev_status_code", F.lag("status_code").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)
