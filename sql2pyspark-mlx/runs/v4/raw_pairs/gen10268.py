from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level")
w_grp = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("key").isin(["version", "trace", "region"]))
    .select(
        "service",
        "host",
        "log_ts",
        "level",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

scored = (
    unpacked
    .select(
        "*",
        F.count("is_flagged").over(w_part).alias("level_count_is_flagged"),
        F.percent_rank().over(w_grp).alias("status_code_high_percent_rank"),
    )
)

result = (
    scored
    .select(
        "log_ts",
        "level",
        "key",
        "level_count_is_flagged",
        "status_code_high_percent_rank",
    )
    .orderBy(F.col("level").desc_nulls_last())
)
