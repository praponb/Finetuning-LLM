from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "key",
        F.when(F.col("host").rlike("^(h1|h3)$"), "GROUP_B").when(F.col("host").rlike("^(h2|h4)$"), "GROUP_A").otherwise("EXTENDED").alias("host_zone"),
    )
    .orderBy("key")
)
