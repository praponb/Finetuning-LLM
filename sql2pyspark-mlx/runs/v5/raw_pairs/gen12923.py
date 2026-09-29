from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("adjusted_is_utm_source_tier").orderBy(F.col("device_rows").desc())

flattened = df_carts.select("id", F.explode("attributes").alias("attr_key", "value"))

base = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.initcap(F.trim("attr_key")).alias("attr_key_title"))
)

base_carts = (
    base
    .filter(F.col("attr_key").eqNullSafe(F.lit("gift")))
    .select(
        "*",
        F.initcap(F.trim("attr_key")).alias("attr_key_title_2"),
        F.when(F.col("attr_key") == "utm_source", 1).otherwise(0).alias("is_utm_source"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

base_carts_2 = (
    base_carts
    .filter(F.col("is_utm_source") <= 30)
    .select(
        "id",
        "attr_key",
        "attr_key_title",
        "attr_key_title_2",
        "is_utm_source",
        "resolved_attr_key",
        F.floor("is_utm_source").alias("is_utm_source_whole"),
        F.round((F.col("is_utm_source") * 100), 2).alias("adjusted_is_utm_source"),
    )
)

result = (
    base_carts_2
    .groupBy(
        F.when(F.col("adjusted_is_utm_source") >= 76, "PREMIUM").otherwise("STANDARD").alias("adjusted_is_utm_source_tier"),
        "attr_key",
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_rows"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.round(F.avg("adjusted_is_utm_source"), 2).alias("avg_adjusted_is_utm_source"),
    )
    .withColumn("device_rows_percent_rank", F.percent_rank().over(w))
)
