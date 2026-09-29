from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w3 = Window.orderBy(F.col("log_ts").desc(), "id", "attr_key")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
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
    exploded
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
    .withColumn("event_seq_2", F.row_number().over(w3))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
    .withColumn("status_code_avg_30m", F.avg("status_code").over(w4))
)
