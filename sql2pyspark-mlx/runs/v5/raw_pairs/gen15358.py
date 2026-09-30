from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_flagged").asc_nulls_last())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_last())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

unpacked = (
    flattened
    .filter(F.to_date("log_ts") >= "2024-11-14")
    .select(
        "id",
        "log_ts",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .filter(F.col("is_flagged") != 0)
    .withColumn("is_flagged_rank", F.rank().over(w))
    .withColumn("status_code_high_max_30m", F.max("status_code_high").over(w2))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_high_count_2h", F.count("status_code_high").over(w4))
    .withColumn("status_code_high_count_2h_2", F.count("status_code_high").over(w4))
)

enriched_app_logs = (
    enriched
    .select(
        "log_ts",
        "value",
        "status_code_high",
        "is_flagged_rank",
        "status_code_high_max_30m",
        "status_code_high_count_2h",
        F.count("is_flagged_rank").over(w_running).alias("is_flagged_rank_count_15m"),
        F.max("is_flagged").over(w_running).alias("is_flagged_max_15m"),
        F.count("is_flagged").over(w_running).alias("is_flagged_count_15m"),
    )
)

grouped = (
    enriched_app_logs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.max("is_flagged_rank_count_15m").alias("max_is_flagged_rank_count_15m"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high_2"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "activity_day",
        "max_is_flagged_rank_count_15m",
        "avg_status_code_high",
        "avg_status_code_high_2",
        "row_count",
    )
    .orderBy(F.col("avg_status_code_high").desc())
)
