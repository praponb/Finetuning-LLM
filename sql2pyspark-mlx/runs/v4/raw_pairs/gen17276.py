from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("latency_ms")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

base = (
    lines
    .filter(F.col("key").isNotNull())
    .withColumn("key_group", F.when(F.col("key").isin(["region", "trace"]), "Priority").otherwise("Other"))
    .withColumn("key_clean", F.coalesce("key", F.lit("NONE")))
)

with_windows = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "attr_value",
        "key_group",
        "key_clean",
        "seq_no",
        "latency_ms_rank",
        F.when(F.col("service").rlike("^(auth|search)$"), "EXTENDED").when(F.col("service").rlike("^(billing|checkout)$"), "GROUP_A").otherwise("CORE").alias("service_zone"),
    )
)
