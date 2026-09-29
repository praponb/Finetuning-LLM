from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("k").orderBy(F.col("is_version").asc_nulls_last())
w3 = Window.partitionBy("k")

lines = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

base = (
    lines
    .filter(F.col("status_code") > 282)
    .withColumn("latency_ms_level", F.when(F.col("latency_ms") >= 2314, "High").when(F.col("latency_ms") >= 577, "Medium").otherwise("Low"))
    .withColumn("latency_ms_rounded", F.round("latency_ms", 0))
    .withColumn("k_filled", F.coalesce("k", F.lit("N/A")))
)

base_app_logs = (
    base
    .filter(F.col("latency_ms") < 942)
    .withColumn("is_version", F.when(F.col("k") == "version", 1).otherwise(0))
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 407, "High").when(F.col("status_code") >= 243, "Medium").otherwise("Low"))
)

scored = (
    base_app_logs
    .withColumn("status_code_percent_rank", F.percent_rank().over(w))
    .withColumn("is_version_rank", F.rank().over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)

result = (
    scored
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "latency_ms_level",
        "latency_ms_rounded",
        "k_filled",
        "status_code_tier",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
