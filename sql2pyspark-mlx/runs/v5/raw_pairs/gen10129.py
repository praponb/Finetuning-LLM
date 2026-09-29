from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy(F.col("status_code_whole").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

base = (
    lines
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("status_code_whole", F.floor("status_code"))
    .withColumn("status_code_str", F.col("status_code").cast("string"))
)

with_windows = (
    base
    .select(
        "id",
        "status_code",
        "attr_key",
        "status_code_whole",
        "status_code_str",
        F.avg("status_code_whole").over(w).alias("attr_key_avg_status_code_whole"),
        F.avg("status_code_whole").over(w2).alias("host_avg_status_code_whole"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
        F.avg("status_code").over(w4).alias("status_code_avg_15m"),
    )
)

valid = (
    with_windows
    .filter(~F.col("attr_key").eqNullSafe(F.lit("region")))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("Unknown")))
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 4))
)

result = (
    valid
    .select(
        "id",
        "status_code",
        "status_code_str",
        "status_code_cume_dist",
        "status_code_avg_15m",
        "attr_key_filled",
        "attr_key_prefix",
        F.count("status_code_cume_dist").over(w).alias("attr_key_count_status_code_cume_dist"),
        F.percent_rank().over(w5).alias("status_code_whole_percent_rank"),
    )
)
