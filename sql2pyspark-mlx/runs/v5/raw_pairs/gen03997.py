from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select(
        "host",
        "level",
        "status_code",
        "k",
        F.sum("status_code").over(w).alias("host_sum_status_code"),
    )
)
