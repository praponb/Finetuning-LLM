from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    exploded
    .select(
        "*",
        F.max("status_code").over(w).alias("status_code_max_30m"),
        F.count("status_code").over(w2).alias("attr_key_count_status_code"),
        F.count("status_code").over(w3).alias("status_code_count_2h"),
    )
)

filtered = (
    with_windows
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("attr_key").isNotNull())
    .withColumn("least_status_code_status_code_count_2h", F.least("status_code", "status_code_count_2h"))
    .withColumn("attr_key_count_status_code_minus_status_code_count_2h", (F.col("attr_key_count_status_code") - F.col("status_code_count_2h")))
)

scored = (
    filtered
    .withColumn("level_avg_attr_key_count_status_code", F.avg("attr_key_count_status_code").over(w4))
    .withColumn("least_status_code_status_code_count_2h_max_15m", F.max("least_status_code_status_code_count_2h").over(w5))
)

base = (
    scored
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("level_avg_attr_key_count_status_code_per_status_code", F.round((F.col("level_avg_attr_key_count_status_code") / F.col("status_code")), 2))
)

result = (
    base
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_value",
        "status_code_max_30m",
        "attr_key_count_status_code",
        "status_code_count_2h",
        "attr_key_count_status_code_minus_status_code_count_2h",
    )
    .orderBy(F.col("log_ts").asc_nulls_first(), F.col("status_code_max_30m").desc())
)
