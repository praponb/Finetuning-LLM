from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_grp = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("flag_bit2").desc())
w4 = Window.partitionBy("service")
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "attr_value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    unpacked
    .withColumn("flag_bit2_avg_30m", F.avg("flag_bit2").over(w_ordered))
    .withColumn("latency_ms_count_1h", F.count("latency_ms").over(w_grp))
    .withColumn("flag_bit2_dense_rank", F.dense_rank().over(w3))
)

enriched = (
    metrics
    .withColumn("latency_ms_count_1h_share", F.round((F.col("latency_ms_count_1h") / F.sum("latency_ms_count_1h").over(w4)), 4))
    .withColumn("flag_bit2_dense_rank_share", F.round((F.col("flag_bit2_dense_rank") / F.sum("flag_bit2_dense_rank").over(w4)), 4))
)

result = (
    enriched
    .withColumn("latency_ms_count_1h_share_2", F.round((F.col("latency_ms_count_1h") / F.sum("latency_ms_count_1h").over(w4)), 4))
    .withColumn("flag_bit2_avg_30m_sum_15m", F.sum("flag_bit2_avg_30m").over(w_hist))
)
