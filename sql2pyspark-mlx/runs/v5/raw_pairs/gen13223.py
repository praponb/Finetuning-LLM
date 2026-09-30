from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").desc(), "id")
w2 = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w_ordered = Window.partitionBy("service")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.corr("latency_ms", "status_code").over(w_ordered).alias("corr_latency_ms_status_code"),
    )
)
