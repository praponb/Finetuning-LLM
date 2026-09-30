from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_search").asc_nulls_last())
w2 = Window.orderBy(F.col("level_length").desc())
w3 = Window.partitionBy("service").orderBy(F.col("level_length").desc())
w4 = Window.partitionBy("service").orderBy(F.col("is_search").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "value"),
    )
)

filtered = (
    lines
    .filter(F.col("key").isNotNull() & (F.col("log_ts") >= "2025-05-01"))
    .withColumn("key_title", F.initcap(F.trim("key")))
)

base = (
    filtered
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "key",
        "value",
        F.lower("key").alias("key_lower"),
        F.length("level").alias("level_length"),
    )
)

filtered_app_logs = (
    base
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "key",
        "value",
        "level_length",
        F.when(F.col("service") == "search", 1).otherwise(0).alias("is_search"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "value",
        "level_length",
        "is_search",
        F.percent_rank().over(w).alias("is_search_percent_rank"),
        F.cume_dist().over(w2).alias("level_length_cume_dist"),
        F.rank().over(w3).alias("level_length_rank"),
        F.percent_rank().over(w4).alias("is_search_percent_rank_2"),
    )
)
