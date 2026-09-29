from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("is_flagged")
w_running = Window.partitionBy("key").orderBy(F.col("is_flagged").desc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc_nulls_first())

flattened = (
    df_app_logs
    .select("id", "status_code", F.explode_outer("context").alias("key", "attr_value"))
)

unpacked = (
    flattened
    .filter(F.col("key").isin(["trace", "version"]))
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

remaining_keys = (
    unpacked
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("service").isin(["checkout", "auth"]))
        .select("id")
    )
)

reconciled = (
    unpacked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        "status_code_high",
        "is_flagged",
        F.floor("status_code").alias("status_code_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("is_flagged_percent_rank"),
        F.percent_rank().over(w_running).alias("is_flagged_percent_rank_2"),
        F.percent_rank().over(w3).alias("status_code_high_percent_rank"),
    )
)

result = (
    scored
    .withColumn("is_flagged_percent_rank_2_tier", F.when(F.col("is_flagged_percent_rank_2") >= 0.77, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["key", "is_flagged_percent_rank_2_tier"], ["key"]],
        "key",
        "is_flagged_percent_rank_2_tier",
    )
    .agg(
        F.count_if(F.col("key") == "version").alias("version_events"),
        F.percentile_approx("status_code_high_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_high_percent_rank_quartiles"),
        F.sum(F.when(F.col("key") == "region", F.col("is_flagged_percent_rank_2")).otherwise(0)).alias("region_is_flagged_percent_rank_2"),
        F.grouping("key").alias("gid"),
    )
)
