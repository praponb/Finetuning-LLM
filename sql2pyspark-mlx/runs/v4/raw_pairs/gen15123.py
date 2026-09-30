from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

valid = (
    flattened
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("level").isin(["ERROR", "WARN"]), "Group A").otherwise("Other").alias("level_group"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .rollup("level", "service")
    .agg(
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.stddev_samp("status_code").alias("status_code_stddev_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("status_code_stddev") < 0)
    .orderBy(F.col("avg_status_code").desc())
)
