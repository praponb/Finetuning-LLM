from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("attr_key").orderBy("status_code")
w6 = Window.orderBy(F.col("status_code_count_30m").asc_nulls_last())
w7 = Window.partitionBy("service").orderBy(F.col("status_code_cume_dist").asc_nulls_last())
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running_2 = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.avg("status_code").over(w).alias("status_code_avg_15m"),
        F.count("status_code").over(w2).alias("status_code_count_30m"),
        F.avg("status_code").over(w3).alias("status_code_avg_30m"),
        F.cume_dist().over(w4).alias("status_code_cume_dist"),
        F.rank().over(w5).alias("status_code_rank"),
    )
)

scored_app_logs = (
    scored
    .withColumn("status_code_count_30m_cume_dist", F.cume_dist().over(w6))
    .withColumn("status_code_cume_dist_percent_rank", F.percent_rank().over(w7))
    .withColumn("status_code_avg_15m_2", F.avg("status_code").over(w8))
    .withColumn("status_code_avg_15m_3", F.avg("status_code").over(w))
)

with_windows = (
    scored_app_logs
    .withColumn("status_code_avg_15m_2_sum_1h", F.sum("status_code_avg_15m_2").over(w_running))
    .withColumn("attr_key_max_status_code_count_30m", F.max("status_code_count_30m").over(w_running_2))
)

matched_keys = (
    with_windows
    .select("id", "service")
    .exceptAll(
        df_app_logs
        .filter(F.col("host").isin(["h2", "h3"]))
        .select("id", "service")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("id", "level")
    .intersectAll(
        df_app_logs
        .filter(F.hour("log_ts").between(11, 17))
        .select("id", "level")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "attr_key",
        "status_code_count_30m",
        "status_code_avg_30m",
        "status_code_rank",
        "status_code_cume_dist_percent_rank",
    )
)
