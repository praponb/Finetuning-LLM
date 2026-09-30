from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host").orderBy("service_length", "id", "k")
w2 = Window.partitionBy("k").orderBy("service_length", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("latency_ms") < 1730)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.length("service").alias("service_length"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_app_logs = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "service", "k", "value", "service_length", "resolved_k")
)

filtered = (
    ranked_app_logs
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("version"))))
    .select(
        "*",
        F.when(F.col("k").isin(["trace", "version"]), "Group A").otherwise("Other").alias("k_group"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("k").isNotNull())
    .withColumn("service_length_rounded", F.round("service_length", 0))
    .withColumn("k_filled", F.coalesce("k", F.lit("OTHER")))
)

result = (
    filtered_app_logs
    .select(
        "id",
        "service",
        "k",
        "service_length",
        "resolved_k",
        "k_group",
        "service_length_rounded",
        "k_filled",
    )
    .orderBy("service_length_rounded")
)
