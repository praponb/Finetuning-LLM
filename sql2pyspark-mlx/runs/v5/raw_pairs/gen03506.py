from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy(F.col("latency_ms").desc(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

deduped = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "host", "attr_key", "attr_value")
)

valid = (
    deduped
    .filter(~F.col("host").isin(["h3", "h4"]))
    .select(
        "*",
        F.upper("host").alias("host_upper"),
        F.concat_ws("-", "host", "attr_key").alias("host_attr_key_key"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

base = (
    valid
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.concat("host", F.lit(" "), "attr_key").alias("host_attr_key"))
)

result = (
    base
    .select(
        "id",
        "attr_key",
        "attr_value",
        "host_upper",
        "host_attr_key_key",
        "attr_key_filled",
        "host_attr_key",
    )
    .orderBy("attr_key")
)
