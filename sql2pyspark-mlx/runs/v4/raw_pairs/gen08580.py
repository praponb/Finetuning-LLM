from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
)
