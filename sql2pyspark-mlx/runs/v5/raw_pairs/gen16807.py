from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "value",
        F.min("status_code").over(w_recent).alias("running_min_status_code"),
        F.max("status_code").over(w_recent).alias("running_max_status_code"),
        F.sum("status_code").over(w_recent).alias("running_sum_status_code"),
        F.count("status_code").over(w_recent).alias("running_count_status_code"),
        F.percent_rank().over(w2).alias("status_code_percent_rank"),
    )
)
