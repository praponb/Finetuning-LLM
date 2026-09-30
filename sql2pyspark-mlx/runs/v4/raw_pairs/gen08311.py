from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_first())
w2 = Window.partitionBy("service", "attr_key")
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("attr_key", "service")
w_running = Window.partitionBy("attr_key").orderBy(F.col("flag_bit2").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "service",
        "attr_key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull())
    .withColumn("flag_bit2_tier", F.when(F.col("flag_bit2") >= 1, "High").when(F.col("flag_bit2") >= 0, "Medium").otherwise("Low"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("status_code_high_rank"),
        F.stddev_samp("flag_bit2").over(w2).alias("service_stddev_samp_flag_bit2"),
        F.sum("flag_bit2").over(w3).alias("service_sum_flag_bit2"),
    )
)

result = (
    with_windows
    .withColumn("status_code_high_rank_share", F.round((F.col("status_code_high_rank") / F.sum("status_code_high_rank").over(w3)), 4))
    .withColumn("attr_key_max_service_stddev_samp_flag_bit2", F.max("service_stddev_samp_flag_bit2").over(w4))
    .withColumn("flag_bit2_rank", F.rank().over(w_running))
)
