from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "level").orderBy("status_code")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("status_code") < 304)
    .select("*", F.round("status_code", 0).alias("status_code_rounded"))
)

metrics = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.sum("status_code_rounded").over(w3).alias("level_sum_status_code_rounded"),
    )
)

valid_app_logs = (
    metrics
    .filter(F.col("status_code_share") >= 0.65)
    .withColumn("status_code_per_status_code_rank", F.round((F.col("status_code") / F.col("status_code_rank")), 2))
)

valid_app_logs_2 = (
    valid_app_logs
    .filter(F.col("status_code") == 409)
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "value",
        "status_code_share",
        "level_sum_status_code_rounded",
        "status_code_per_status_code_rank",
        F.round("status_code_per_status_code_rank", 2).alias("status_code_per_status_code_rank_rounded"),
        F.upper("level").alias("level_upper"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

grouped = (
    valid_app_logs_2
    .groupBy(
        F.when(F.col("status_code_share") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("status_code_share_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("key").alias("distinct_keys"),
        F.sum(F.when(F.col("level") == "ERROR", F.col("status_code")).otherwise(0)).alias("error_status_code"),
        F.sum(F.when(F.col("level") == "ERROR", F.col("level_sum_status_code_rounded")).otherwise(0)).alias("error_level_sum_status_code_rounded"),
        F.sum("status_code_per_status_code_rank_rounded").alias("total_status_code_per_status_code_rank_rounded"),
        F.percentile_approx("status_code_per_status_code_rank", 0.9).alias("status_code_per_status_code_rank_p90"),
    )
)

result = (
    grouped
    .select(
        "status_code_share_tier",
        "distinct_keys",
        "total_status_code_per_status_code_rank_rounded",
        "status_code_per_status_code_rank_p90",
    )
    .orderBy(F.col("total_status_code_per_status_code_rank_rounded").desc_nulls_first())
)
