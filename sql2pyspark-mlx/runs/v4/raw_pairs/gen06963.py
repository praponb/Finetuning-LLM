from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key").orderBy(F.col("latency_ms").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.row_number().over(w_recent).alias("event_seq"),
        F.lead("latency_ms", 2).over(w2).alias("next_latency_ms"),
        F.sum("latency_ms").over(w3).alias("running_latency_ms"),
    )
)
