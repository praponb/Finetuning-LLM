from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "k",
        "attr_value",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
    )
)
