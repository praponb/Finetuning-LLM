from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "key").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "attr_value",
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.lead("latency_ms").over(w).alias("next_latency_ms"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)
