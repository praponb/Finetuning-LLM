from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("host").orderBy("status_code", "id").rowsBetween(-3, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .withColumn("last4_max_status_code", F.max("status_code").over(w_hist))
    .withColumn("last4_count_status_code", F.count("status_code").over(w_hist))
    .withColumn("last4_sum_status_code", F.sum("status_code").over(w_hist))
)
