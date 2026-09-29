from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("days_since_log_ts").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "v"),
    )
)

filtered = (
    lines
    .filter(F.col("log_ts") < "2024-07-19")
    .withColumn("days_since_log_ts", F.datediff(F.current_date(), "log_ts"))
    .withColumn("key_filled", F.coalesce("key", F.lit("OTHER")))
)

cleaned = (
    filtered
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 15:00:00"))) & (F.col("key") != "trace"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "v",
        "days_since_log_ts",
        "key_filled",
        F.to_date("log_ts").alias("log_ts_date"),
    )
)

result = (
    cleaned
    .withColumn("days_since_log_ts_max_15m", F.max("days_since_log_ts").over(w))
    .withColumn("days_since_log_ts_dense_rank", F.dense_rank().over(w2))
)
