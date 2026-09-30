from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("host", "service").orderBy("latency_ms", "id").rowsBetween(-3, -1)

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") > 1002)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "host",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_3_prior"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_3_prior_2"),
    )
)
