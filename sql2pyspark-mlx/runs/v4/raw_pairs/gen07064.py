from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("latency_ms_avg_3").asc_nulls_first())
w4 = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

enriched = (
    exploded
    .withColumn("latency_ms_avg_3", F.avg("latency_ms").over(w))
    .withColumn("first_known_latency_ms", F.first("latency_ms", ignorenulls=True).over(w2))
)

enriched_app_logs = (
    enriched
    .withColumn("latency_ms_avg_3_cume_dist", F.cume_dist().over(w3))
    .withColumn("prev_latency_ms_avg_3", F.lag("latency_ms_avg_3", 1).over(w4))
    .withColumn("cumulative_latency_ms_avg_3", F.sum("latency_ms_avg_3").over(w5))
)

result = (
    enriched_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "value",
        "latency_ms_avg_3",
        "first_known_latency_ms",
    )
    .orderBy("id", "latency_ms_avg_3")
)
