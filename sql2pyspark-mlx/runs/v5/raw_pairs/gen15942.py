from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service").orderBy("latency_ms")
w2 = Window.partitionBy("service", "key").orderBy(F.col("latency_ms").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "attr_value",
        F.percent_rank().over(w_ordered).alias("latency_ms_percent_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)
