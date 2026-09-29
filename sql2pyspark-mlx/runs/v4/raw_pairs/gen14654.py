from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "attr_key")
w2 = Window.orderBy(F.col("latency_ms").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)
