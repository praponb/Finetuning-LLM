from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("service").orderBy("status_code_high", "id", "key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("service", "key").orderBy(F.col("flag_bit8").desc_nulls_last(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    unpacked
    .select(
        "*",
        F.lag("flag_bit8", 1).over(w_part).alias("prev_flag_bit8"),
        F.stddev_samp("status_code_high").over(w2).alias("service_stddev_samp_status_code_high"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "value",
        "flag_bit8",
        "status_code_high",
        "prev_flag_bit8",
        "latest_rank",
    )
)
