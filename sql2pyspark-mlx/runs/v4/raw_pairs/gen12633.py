from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code")
w_hist = Window.partitionBy("level", "key")
w_running = Window.orderBy(F.col("status_code").desc())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("level", "service")
w6 = Window.partitionBy("level").orderBy(F.col("status_code_percent_rank").asc_nulls_first())
w7 = Window.partitionBy("service").orderBy(F.col("level_max_status_code").desc_nulls_first())
w8 = Window.partitionBy("service").orderBy("status_code_count_30m")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

scored = (
    lines
    .withColumn("status_code_dense_rank", F.dense_rank().over(w))
    .withColumn("level_sum_status_code", F.sum("status_code").over(w_hist))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_running))
    .withColumn("status_code_count_30m", F.count("status_code").over(w4))
    .withColumn("level_max_status_code", F.max("status_code").over(w5))
)

filtered = (
    scored
    .filter(F.col("level_sum_status_code") > 1402)
    .withColumn("service_upper", F.upper("service"))
    .withColumn("status_code_str", F.col("status_code").cast("double"))
    .withColumn("key_clean", F.coalesce("key", F.lit("N/A")))
)

metrics = (
    filtered
    .withColumn("status_code_percent_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("level_max_status_code_rank", F.rank().over(w7))
    .withColumn("status_code_count_30m_dense_rank", F.dense_rank().over(w8))
)

result = (
    metrics
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "level_sum_status_code",
        "status_code_count_30m",
        "service_upper",
        "key_clean",
        F.when(F.col("level").rlike("^(INFO|ERROR)$"), "GROUP_B").otherwise("GROUP_A").alias("level_zone"),
    )
)
