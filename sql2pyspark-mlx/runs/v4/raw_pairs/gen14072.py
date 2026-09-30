from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("service").orderBy("status_code", "id", "key")
w3 = Window.partitionBy("host", "key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.lead("latency_ms", 1).over(w2).alias("next_latency_ms"),
        F.sum("latency_ms").over(w3).alias("running_latency_ms"),
    )
)
