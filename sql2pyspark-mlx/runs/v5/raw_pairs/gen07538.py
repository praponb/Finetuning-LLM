from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

deduped = (
    flattened
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "key", "value")
)

base = (
    deduped
    .filter((F.col("key") == "trace") & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .withColumn("key_filled", F.coalesce("key", F.lit("unassigned")))
)

cleaned = (
    base
    .filter(F.col("key").isNotNull() & (F.col("key") != "version"))
    .select(
        "*",
        F.substring("key", 1, 4).alias("key_prefix"),
        F.when(F.col("key").isin(["region", "version"]), "Group A").otherwise("Other").alias("key_group"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(~F.col("key").isin(["trace", "region"]))
    .select(
        "id",
        "key",
        "value",
        "key_filled",
        "key_prefix",
        "key_group",
        F.initcap(F.trim("key")).alias("key_title"),
    )
)

result = (
    cleaned_app_logs
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.countDistinct("key").alias("distinct_keys"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("region_count")
)
