from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code_high").asc_nulls_last(), "id", "k")
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy(F.col("adjusted_status_code_high_per_is_flagged").desc(), "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("adjusted_status_code_high_per_is_flagged_bucket").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

valid = (
    unpacked
    .filter(F.col("k").isNotNull())
    .withColumn("k_filled", F.coalesce("k", F.lit("unassigned")))
    .withColumn("status_code_high_per_is_flagged", (F.col("status_code_high") / F.nullif("is_flagged", F.lit(0))))
    .withColumn("k_clean", F.coalesce("k", F.lit("N/A")))
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "k",
        "status_code_high",
        "is_flagged",
        "k_filled",
        "status_code_high_per_is_flagged",
        "k_clean",
        F.round((F.col("status_code_high_per_is_flagged") * 1.15), 2).alias("adjusted_status_code_high_per_is_flagged"),
        F.when(F.col("status_code_high") >= 2, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low").alias("status_code_high_band"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

enriched = (
    filtered
    .filter(F.col("status_code_high") > 1)
    .select(
        "id",
        "k",
        "status_code_high",
        "k_filled",
        "status_code_high_per_is_flagged",
        "status_code_high_band",
        F.count("is_flagged").over(w2).alias("k_count_is_flagged"),
        F.ntile(10).over(w3).alias("adjusted_status_code_high_per_is_flagged_bucket"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

daily = (
    ranked
    .groupBy(
        F.when(F.col("status_code_high_per_is_flagged") >= 63, "PREMIUM").otherwise("STANDARD").alias("status_code_high_per_is_flagged_tier"),
    )
    .agg(
        F.avg("status_code_high").alias("avg_status_code_high"),
        F.count_if(F.col("k") == "region").alias("region_events"),
        F.percentile_approx("status_code_high", 0.5).alias("status_code_high_p50"),
    )
)

result = (
    daily
    .select(
        "status_code_high_per_is_flagged_tier",
        "avg_status_code_high",
        "status_code_high_p50",
    )
    .orderBy(
        F.col("status_code_high_per_is_flagged_tier").desc_nulls_last(),
        "status_code_high_p50",
    )
)
