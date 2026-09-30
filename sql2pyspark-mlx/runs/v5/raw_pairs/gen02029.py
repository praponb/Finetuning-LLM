from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("key").orderBy("status_code")
w_running = Window.partitionBy("key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .filter(F.col("service") == "auth")
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w_running))
)
