from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_last(), "id", "k")
w4 = Window.partitionBy("host").orderBy(F.col("new_group_flag").desc_nulls_first(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

with_gaps = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

latest = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_app_logs = (
    latest
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "status_code", "k", "value")
)

result = (
    latest_app_logs
    .select("id", "status_code", "k", "value")
    .orderBy(F.col("status_code").desc_nulls_first())
)
