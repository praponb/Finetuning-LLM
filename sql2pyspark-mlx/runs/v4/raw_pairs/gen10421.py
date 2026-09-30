from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("is_utm_source").asc_nulls_last())
w2 = Window.partitionBy("attr_key").orderBy(F.col("is_utm_source").asc_nulls_first())
w3 = Window.partitionBy("attr_key")

lines = df_carts.select("id", F.explode_outer("attributes").alias("attr_key", "v"))

filtered = (
    lines
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") == "device"))
    .select("*", F.lower("attr_key").alias("attr_key_lower"))
)

cleaned = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_lower_2", F.lower("attr_key"))
    .withColumn("is_utm_source", F.when(F.col("attr_key") == "utm_source", 1).otherwise(0))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("is_utm_source").between(32, 79))
    .withColumn("is_utm_source_whole", F.floor("is_utm_source"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("N/A")))
)

valid = (
    cleaned_carts
    .filter(F.col("is_utm_source_whole").between(37, 71))
    .select(
        "*",
        F.round("is_utm_source_whole", 1).alias("is_utm_source_whole_rounded"),
        F.upper("attr_key").alias("attr_key_upper"),
    )
)

scored = (
    valid
    .withColumn("is_utm_source_dense_rank", F.dense_rank().over(w))
    .withColumn("is_utm_source_rank", F.rank().over(w2))
    .withColumn("attr_key_stddev_samp_is_utm_source", F.stddev_samp("is_utm_source").over(w3))
)

valid_carts = (
    scored
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "attr_key",
        "is_utm_source",
        "attr_key_clean",
        "is_utm_source_whole",
        "resolved_attr_key",
        "is_utm_source_rank",
        "attr_key_stddev_samp_is_utm_source",
        F.round("is_utm_source_whole_rounded", 2).alias("is_utm_source_whole_rounded_rounded"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_clean_2"),
    )
)

result = (
    valid_carts
    .groupBy("attr_key")
    .agg(
        F.round(F.avg("attr_key_stddev_samp_is_utm_source"), 2).alias("avg_attr_key_stddev_samp_is_utm_source"),
        F.sum(F.when(F.col("attr_key") == "gift", F.col("is_utm_source_whole_rounded_rounded")).otherwise(0)).alias("gift_is_utm_source_whole_rounded_rounded"),
    )
    .filter(F.col("avg_attr_key_stddev_samp_is_utm_source") < 0)
)
