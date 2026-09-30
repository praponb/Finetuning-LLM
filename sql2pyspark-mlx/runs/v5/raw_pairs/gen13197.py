from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("service")
w2 = Window.orderBy("status_code")
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("key").orderBy("status_code")
w6 = Window.orderBy("service_sum_status_code")
w7 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w8 = Window.orderBy(F.col("service_sum_status_code").asc_nulls_last())
w9 = Window.orderBy(F.col("status_code_rank").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("status_code") > 325)
    .withColumn("service_prefix", F.substring("service", 1, 3))
    .withColumn("resolved_key", F.coalesce("key", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .withColumn("service_sum_status_code", F.sum("status_code").over(w_grp))
    .withColumn("status_code_rank", F.rank().over(w2))
    .withColumn("key_count_status_code", F.count("status_code").over(w3))
    .withColumn("host_stddev_samp_status_code", F.stddev_samp("status_code").over(w4))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w5))
)

valid = (
    with_windows
    .filter(F.col("key").isNotNull() & (F.col("service_sum_status_code") >= 1654))
    .withColumn("key_host", F.concat("key", F.lit("/"), "host"))
    .withColumn("key_filled", F.coalesce("key", F.lit("N/A")))
    .withColumn("key_clean", F.coalesce("key", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .filter(F.col("host_stddev_samp_status_code") >= 439)
    .withColumn("service_sum_status_code_percent_rank", F.percent_rank().over(w6))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w7))
    .withColumn("service_sum_status_code_rank", F.rank().over(w8))
    .withColumn("status_code_rank_cume_dist", F.cume_dist().over(w9))
)

result = (
    metrics
    .select(
        "id",
        "service",
        "host",
        "key",
        "attr_value",
        "service_prefix",
        "resolved_key",
        "key_filled",
        "key_clean",
        F.when(F.col("key").rlike("^(version|trace)$"), "OTHER_ZONE").when(F.col("key").rlike("^(region)$"), "GROUP_A").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy("service")
)
