from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("attr_key")
w_grp = Window.partitionBy("attr_key", "host")
w4 = Window.partitionBy("host").orderBy(F.col("service_min_status_code").desc_nulls_last())
w5 = Window.partitionBy("service").orderBy(F.col("service_min_status_code").desc())
w6 = Window.partitionBy("host", "attr_key")
w7 = Window.partitionBy("attr_key").orderBy(F.col("attr_key_max_status_code").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

filtered = (
    exploded
    .filter(F.col("attr_key") == "version")
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        "value",
        F.when(F.col("status_code") >= 424, "High").when(F.col("status_code") >= 242, "Medium").otherwise("Low").alias("status_code_tier"),
    )
)

metrics = filtered.withColumn("service_min_status_code", F.min("status_code").over(w))

with_windows = (
    metrics
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.max("status_code").over(w_grp).alias("attr_key_max_status_code"),
    )
)

with_windows_app_logs = (
    with_windows
    .withColumn("service_min_status_code_dense_rank", F.dense_rank().over(w4))
    .withColumn("service_min_status_code_rank", F.rank().over(w5))
    .withColumn("host_stddev_samp_status_code", F.stddev_samp("status_code").over(w6))
    .withColumn("attr_key_max_status_code_dense_rank", F.dense_rank().over(w7))
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "attr_key",
        "value",
        "status_code_tier",
        "service_min_status_code",
        "attr_key_max_status_code",
        F.when(F.col("host").rlike("^(h3|h1)$"), "GROUP_B").when(F.col("host").rlike("^(h2|h4)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("host_zone"),
    )
)
