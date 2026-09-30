from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w_ordered = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.orderBy("status_code")
w4 = Window.partitionBy("service").orderBy(F.col("status_code_dense_rank").desc_nulls_first())
w5 = Window.partitionBy("attr_key")
w6 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy("service").orderBy(F.col("status_code_dense_rank").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.avg("status_code").over(w_ordered).alias("status_code_avg_15m"),
        F.max("status_code").over(w_ordered).alias("status_code_max_15m"),
        F.dense_rank().over(w3).alias("status_code_dense_rank"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w4).alias("status_code_dense_rank_dense_rank"),
        F.round((F.col("status_code_avg_15m") / F.sum("status_code_avg_15m").over(w5)), 4).alias("status_code_avg_15m_share"),
    )
)

enriched = (
    with_windows
    .withColumn("status_code_avg_15m_share_max_15m", F.max("status_code_avg_15m_share").over(w6))
    .withColumn("status_code_dense_rank_dense_rank_2", F.dense_rank().over(w7))
)

grouped = (
    enriched
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.round(F.avg("status_code_cume_dist"), 2).alias("avg_status_code_cume_dist"),
        F.count("*").alias("row_count"),
        F.min("log_ts").alias("first_log_ts"),
        F.round(F.avg("status_code_dense_rank_dense_rank"), 2).alias("avg_status_code_dense_rank_dense_rank"),
    )
)

result = grouped.select("activity_day", "avg_status_code_cume_dist", "first_log_ts")
