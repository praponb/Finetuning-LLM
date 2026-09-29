from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("attr_key", "host").orderBy(F.col("latency_ms").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "attr_key",
        F.lead("latency_ms").over(w_recent).alias("next_latency_ms"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)
