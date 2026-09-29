from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("k_length").asc_nulls_first())
w2 = Window.orderBy("k_length")
w3 = Window.partitionBy("k", "service")
w4 = Window.orderBy(F.col("k_length").desc())

flattened = (
    df_app_logs
    .select("id", "service", "level", F.explode_outer("context").alias("k", "v"))
)

valid = (
    flattened
    .filter(F.col("level").isin(["INFO", "DEBUG", "WARN"]))
    .select(
        "service",
        "level",
        "k",
        F.concat_ws("-", "service", "level").alias("service_level_key"),
    )
)

base = (
    valid
    .filter(F.col("k").isNotNull() & (F.col("k") == "version"))
    .select("*", F.initcap(F.trim("level")).alias("level_title"))
)

base_app_logs = (
    base
    .filter(F.col("k").isin(["trace", "version", "region"]))
    .select(
        "service",
        "k",
        "service_level_key",
        "level_title",
        F.lower("service").alias("service_lower"),
        F.length("k").alias("k_length"),
    )
)

scored = (
    base_app_logs
    .withColumn("k_length_percent_rank", F.percent_rank().over(w))
    .withColumn("k_length_cume_dist", F.cume_dist().over(w2))
    .withColumn("k_avg_k_length", F.avg("k_length").over(w3))
    .withColumn("k_length_rank", F.rank().over(w4))
)

daily = (
    scored
    .groupBy(
        "service",
        F.when(F.col("k_length") >= 60, "HIGH").otherwise("LOW").alias("k_length_tier"),
    )
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.countDistinct("k").alias("distinct_ks"),
    )
)

result = daily.select("service", "k_length_tier", "num_services", "distinct_ks")
