from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

base = (
    flattened
    .filter(F.col("k").isNotNull() & (F.col("service") == "billing"))
    .withColumn("k_group", F.when(F.col("k").isin(["region", "trace"]), "Priority").otherwise("Other"))
    .withColumn("is_h4", F.when(F.col("host") == "h4", 1).otherwise(0))
    .withColumn("k_filled", F.coalesce("k", F.lit("N/A")))
)

valid = (
    base
    .filter(F.col("host") == "h4")
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
    .withColumn("adjusted_is_h4", F.round((F.col("is_h4") * 100), 2))
    .withColumn("resolved_k", F.coalesce("k", F.lit("N/A")))
)

filtered = (
    valid
    .filter(F.col("k").isNotNull())
    .withColumn("clean_k", F.trim("k"))
)

valid_app_logs = (
    filtered
    .filter(F.col("k").isNotNull() & (F.col("k") == "trace"))
    .select(
        "service",
        "latency_ms",
        "status_code",
        "k_group",
        "is_h4",
        "k_filled",
        "latency_ms_filled",
        "adjusted_is_h4",
        "resolved_k",
        "clean_k",
        F.col("adjusted_is_h4").cast("int").alias("adjusted_is_h4_int"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled_2"),
    )
)

result = (
    valid_app_logs
    .filter(F.col("latency_ms_filled") <= 20)
    .select(
        "service",
        "status_code",
        "is_h4",
        "k_filled",
        "resolved_k",
        F.min("adjusted_is_h4").over(w).alias("service_min_adjusted_is_h4"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)
