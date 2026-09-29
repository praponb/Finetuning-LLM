from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "key")
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_running = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_part = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_recent = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("level")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.max("status_code").over(w).alias("level_max_status_code"),
        F.avg("status_code").over(w2).alias("status_code_avg_15m"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.count("status_code").over(w_running).alias("status_code_count_1h"),
        F.avg("status_code").over(w_part).alias("status_code_avg_15m_2"),
        F.count("status_code_avg_15m").over(w_recent).alias("status_code_avg_15m_count_30m"),
        F.round((F.col("status_code_avg_15m") / F.sum("status_code_avg_15m").over(w6)), 4).alias("status_code_avg_15m_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "key",
        "attr_value",
        "level_max_status_code",
        "status_code_avg_15m",
        "status_code_count_1h",
        F.when(F.col("key").rlike("^(version|trace)$"), "CORE").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("log_ts").asc_nulls_first())
)
