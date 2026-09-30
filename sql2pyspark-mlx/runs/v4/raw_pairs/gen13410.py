from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code")
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w5 = Window.orderBy(F.col("status_code").desc())
w6 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("key").orderBy(F.col("status_code_percent_rank").asc_nulls_first())
w8 = Window.orderBy(F.col("status_code_cume_dist").asc_nulls_last())
w_running = Window.partitionBy("key").orderBy(F.col("status_code_cume_dist").desc_nulls_last())
w_recent = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

scored = (
    lines
    .withColumn("status_code_percent_rank", F.percent_rank().over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
    .withColumn("status_code_max_2h", F.max("status_code").over(w3))
    .withColumn("status_code_rank", F.rank().over(w4))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w5))
)

scored_app_logs = (
    scored
    .withColumn("status_code_percent_rank_avg_30m", F.avg("status_code_percent_rank").over(w6))
    .withColumn("status_code_percent_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("status_code_cume_dist_percent_rank", F.percent_rank().over(w8))
)

result = (
    scored_app_logs
    .withColumn("status_code_cume_dist_percent_rank_2", F.percent_rank().over(w_running))
    .withColumn("status_code_rank_share", F.round((F.col("status_code_rank") / F.sum("status_code_rank").over(w2)), 4))
    .withColumn("status_code_cume_dist_percent_rank_max_2h", F.max("status_code_cume_dist_percent_rank").over(w_recent))
    .withColumn("status_code_share_sum_2h", F.sum("status_code_share").over(w_recent))
)
