from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("service").orderBy("k_avg_host_length")
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("k").orderBy(F.col("host_length").desc())
w6 = Window.partitionBy("host").orderBy(F.col("host_length_dense_rank").desc())
w7 = Window.orderBy(F.col("k_avg_host_length").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

base = (
    exploded
    .filter(F.col("k").isin(["trace", "version", "region"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "k",
        "value",
        F.concat_ws("/", "k", "level").alias("k_level_key"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

kept_keys = (
    base
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") != 263)
        .select("host", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("service").isin(["checkout", "search"]))
    .withColumn("host_length", F.length("host"))
    .withColumn("host_group", F.when(F.col("host").isin(["h1", "h3"]), "Priority").otherwise("Other"))
    .withColumn("k_clean_2", F.coalesce("k", F.lit("DIRECT")))
)

enriched = (
    valid
    .withColumn("k_avg_host_length", F.avg("host_length").over(w))
    .withColumn("level_count_host_length", F.count("host_length").over(w2))
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.max("host_length").over(w).alias("k_max_host_length"),
        F.rank().over(w3).alias("k_avg_host_length_rank"),
        F.avg("host_length").over(w4).alias("service_avg_host_length"),
        F.dense_rank().over(w5).alias("host_length_dense_rank"),
        F.stddev_samp("host_length").over(w2).alias("level_stddev_samp_host_length"),
    )
)

result = (
    enriched_app_logs
    .withColumn("host_length_dense_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("k_avg_host_length_cume_dist", F.cume_dist().over(w7))
)
