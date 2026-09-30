from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("log_ts", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context").getItem("region").alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .withColumn("last3_avg_latency_ms", F.avg("latency_ms").over(w))
    .withColumn("last3_count_latency_ms", F.count("latency_ms").over(w))
    .withColumn("last3_sum_latency_ms", F.sum("latency_ms").over(w))
    .withColumn("next_latency_ms", F.lead("latency_ms", 2).over(w2))
)
