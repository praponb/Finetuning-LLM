from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service")
w_grp = Window.orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w4 = Window.partitionBy("level").orderBy(F.col("latency_ms").asc_nulls_first())
w5 = Window.partitionBy("attr_key").orderBy("latency_ms_bucket", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("latency_ms_percent_rank", "id", "attr_key").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("attr_key")
w_hist = Window.partitionBy("service")
w9 = Window.partitionBy("service").orderBy("latency_ms")
w10 = Window.partitionBy("service", "level").orderBy(F.col("latency_ms_bucket").desc(), "id", "attr_key")
w11 = Window.partitionBy("level").orderBy(F.col("latency_ms_share_avg_5").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

enriched = (
    lines
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("last_known_latency_ms", F.last("latency_ms", ignorenulls=True).over(w_grp))
    .withColumn("latency_ms_bucket", F.ntile(10).over(w_grp_2))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w4))
)

metrics = (
    enriched
    .filter(F.col("service") == "checkout")
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("latency_ms_share_avg_5", F.avg("latency_ms_share").over(w6))
    .withColumn("attr_key_avg_latency_ms_share", F.avg("latency_ms_share").over(w7))
    .withColumn("service_count_latency_ms_bucket", F.count("latency_ms_bucket").over(w_hist))
)

scored = (
    metrics
    .withColumn("latency_ms_percent_rank_2", F.percent_rank().over(w9))
    .withColumn("event_seq_2", F.row_number().over(w10))
    .withColumn("latency_ms_share_avg_5_cume_dist", F.cume_dist().over(w11))
)

result = (
    scored
    .groupBy("attr_key")
    .pivot("service", ["search", "billing", "checkout"])
    .agg(F.avg("event_seq_2").alias("amount"), F.count(F.lit(1)).alias("n"))
)
