from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("attr_key")
w_running = Window.orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.max("status_code").over(w_part).alias("attr_key_max_status_code"),
        F.percent_rank().over(w_running).alias("status_code_percent_rank"),
    )
)
