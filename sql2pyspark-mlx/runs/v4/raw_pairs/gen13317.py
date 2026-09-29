from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("service").orderBy("status_code", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.lag("latency_ms", 2).over(w2).alias("prev_latency_ms"),
    )
)
