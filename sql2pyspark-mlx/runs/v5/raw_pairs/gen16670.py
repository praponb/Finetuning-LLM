from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code")
w2 = Window.partitionBy("key").orderBy("rn")
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms").asc_nulls_first(), "id", "key")
w4 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("service", "host").orderBy("rn", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

ranked = (
    flattened
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("rn_2", F.rank().over(w2))
    .filter(F.col("rn_2") == 1)
)

ranked_app_logs = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "rn",
        "rn_2",
        F.sum("latency_ms").over(w4).alias("cumulative_latency_ms"),
        F.sum("status_code").over(w5).alias("cumulative_status_code"),
    )
)

result = with_windows.select("id", "latency_ms", "status_code", "key", "rn", "rn_2")
