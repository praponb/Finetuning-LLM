from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy("num_context", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("service").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "payload_json",
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .select(
        "id",
        "service",
        "latency_ms",
        "num_context",
        "user_id",
        F.sum("num_context").over(w).alias("host_sum_num_context"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_6_prior"),
        F.last("num_context", ignorenulls=True).over(w3).alias("last_known_num_context"),
    )
)
