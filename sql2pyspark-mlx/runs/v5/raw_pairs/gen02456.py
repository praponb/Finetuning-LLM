from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "host").orderBy("latency_ms", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("host")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "context",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .select(
        "id",
        "duration_text",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_3_prior"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
    )
)
