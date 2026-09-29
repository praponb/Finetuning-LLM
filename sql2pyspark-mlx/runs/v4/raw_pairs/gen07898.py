from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    flattened
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("latency_ms_max_1h", F.max("latency_ms").over(w2))
)
