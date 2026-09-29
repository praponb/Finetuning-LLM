from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.orderBy("status_code_rank_2")
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w8 = Window.partitionBy("level")
w9 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.rank().over(w2).alias("status_code_rank_2"),
        F.max("status_code").over(w3).alias("status_code_max_2h"),
    )
)

scored_app_logs = (
    scored
    .withColumn("status_code_rank_avg_1h", F.avg("status_code_rank").over(w4))
    .withColumn("status_code_rank_sum_30m", F.sum("status_code_rank").over(w5))
    .withColumn("status_code_rank_2_rank", F.rank().over(w6))
)

metrics = (
    scored_app_logs
    .select(
        "*",
        F.avg("status_code_rank_avg_1h").over(w5).alias("status_code_rank_avg_1h_avg_30m"),
        F.sum("status_code_rank_sum_30m").over(w_hist).alias("status_code_rank_sum_30m_sum_1h"),
        F.round((F.col("status_code_rank_avg_1h") / F.sum("status_code_rank_avg_1h").over(w8)), 4).alias("status_code_rank_avg_1h_share"),
        F.round((F.col("status_code_rank_2") / F.sum("status_code_rank_2").over(w9)), 4).alias("status_code_rank_2_share"),
    )
)

cleaned = (
    metrics
    .filter(F.col("k").isNotNull() & F.col("service").isin(["checkout", "search"]))
    .select(
        "*",
        F.ceil("status_code_rank_2_share").alias("status_code_rank_2_share_whole"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "k",
        "v",
        "status_code_rank",
        "status_code_rank_2",
        "status_code_rank_avg_1h",
        F.when(F.col("k").rlike("^(region|version)$"), "GROUP_B").otherwise("PRIORITY").alias("k_zone"),
    )
)
