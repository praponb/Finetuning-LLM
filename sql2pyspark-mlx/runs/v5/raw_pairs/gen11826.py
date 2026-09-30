from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

valid = (
    exploded
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        F.lower("host").alias("host_lower"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

metrics = valid.withColumn("latency_ms_dense_rank", F.dense_rank().over(w))

valid_app_logs = (
    metrics
    .filter(F.col("latency_ms_dense_rank").between(4, 6))
    .select(
        "host",
        "latency_ms",
        "status_code",
        "k",
        "latency_ms_dense_rank",
        F.initcap(F.trim("k")).alias("k_title"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled_2"),
    )
)

result = (
    valid_app_logs
    .select(
        "host",
        "latency_ms",
        "status_code",
        "k",
        "k_title",
        F.when(F.col("k").rlike("^(region|trace)$"), "GROUP_A").otherwise("GROUP_B").alias("k_zone"),
    )
    .orderBy(F.col("latency_ms").desc())
)
