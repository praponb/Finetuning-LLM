from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id")
w2 = Window.partitionBy("service").orderBy("status_code_high", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_last())

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .withColumn("prev_latency_ms", F.lag("latency_ms", 1).over(w))
    .withColumn("status_code_high_avg_3", F.avg("status_code_high").over(w2))
    .withColumn("running_status_code_high", F.sum("status_code_high").over(w3))
    .withColumn("latency_ms_rank", F.rank().over(w4))
)
