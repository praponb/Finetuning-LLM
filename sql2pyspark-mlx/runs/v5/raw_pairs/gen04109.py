from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("level_length").desc(), "id", "attr_key")
w2 = Window.partitionBy("level").orderBy(F.col("latest_rank").desc(), "id", "attr_key")

lines = df_app_logs.select("id", "level", F.explode("context").alias("attr_key", "value"))

base = (
    lines
    .filter(F.col("attr_key").isin(["version", "region", "trace"]))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("unassigned")))
    .withColumn("attr_key_filled_2", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

base_app_logs = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "level",
        "attr_key",
        "value",
        "attr_key_filled",
        "attr_key_filled_2",
        F.substring("level", 1, 4).alias("level_prefix"),
    )
)

cleaned = (
    base_app_logs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "attr_key", "level").alias("attr_key_level_key"),
        F.length("level").alias("level_length"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled_3"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    latest
    .filter(~F.col("attr_key").eqNullSafe(F.lit("version")))
    .select(
        "*",
        F.floor("level_length").alias("level_length_whole"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("resolved_attr_key"),
    )
)

daily = (
    filtered
    .withColumn("level_length_tier", F.when(F.col("level_length") >= 68, "HIGH").otherwise("LOW"))
    .rollup("level_length_tier", "attr_key")
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.min("level_length").alias("min_level_length"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    daily
    .select("attr_key", "attr_key_list")
    .orderBy(F.col("attr_key").desc_nulls_first())
)
