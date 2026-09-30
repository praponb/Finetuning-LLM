from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("is_region_str", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("row_num").desc())

flattened = (
    df_app_logs
    .select("id", "host", F.explode("context").alias("attr_key", "attr_value"))
)

cleaned = (
    flattened
    .filter(F.col("host") == "h2")
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 4))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "attr_key",
        "attr_value",
        "attr_key_prefix",
        F.concat_ws("/", "attr_key", "host").alias("attr_key_host_key"),
    )
)

filtered = (
    cleaned_app_logs
    .filter(F.col("attr_key").isNotNull() & F.col("attr_key").isin(["trace", "region"]))
    .select(
        "id",
        "host",
        "attr_key",
        "attr_value",
        "attr_key_prefix",
        "attr_key_host_key",
        F.when(F.col("attr_key") == "region", 1).otherwise(0).alias("is_region"),
        F.when(F.col("attr_key").isin(["region", "trace"]), "Core").otherwise("Other").alias("attr_key_group"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("attr_key_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("is_region") == 46)
    .select(
        "id",
        "host",
        "attr_key",
        "attr_value",
        "attr_key_host_key",
        "attr_key_group",
        "attr_key_clean",
        F.col("is_region").cast("double").alias("is_region_str"),
        F.col("is_region").cast("string").alias("is_region_str_2"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "host",
        "attr_key",
        "attr_value",
        "attr_key_host_key",
        "attr_key_group",
        "attr_key_clean",
        "is_region_str",
        "is_region_str_2",
    )
    .orderBy(F.col("is_region_str").desc_nulls_first())
)
