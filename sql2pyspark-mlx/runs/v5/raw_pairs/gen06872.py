from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("level").orderBy("latency_ms")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "service",
        "level",
        "k",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.max("status_code").over(w2).alias("status_code_max_1h"),
        F.sum("status_code").over(w3).alias("service_sum_status_code"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
    )
)
