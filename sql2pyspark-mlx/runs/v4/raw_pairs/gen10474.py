from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key")
w2 = Window.partitionBy("host", "key")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("status_code_share").desc_nulls_first())
w_hist = Window.partitionBy("service")
w_ordered = Window.partitionBy("key", "service")
w7 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("host").isin(["h2", "h3", "h4"]))
    .select(
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

with_windows = (
    flattened_attrs
    .withColumn("key_count_flag_bit1", F.count("flag_bit1").over(w_recent))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
    .withColumn("key_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w_recent))
    .withColumn("status_code_count_1h", F.count("status_code").over(w3))
)

result = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w4).alias("status_code_share_dense_rank"),
        F.count("status_code_share").over(w_hist).alias("service_count_status_code_share"),
        F.count("status_code").over(w_ordered).alias("key_count_status_code"),
        F.sum("status_code_high").over(w7).alias("status_code_high_sum_30m"),
    )
)
