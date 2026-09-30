from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy("days_since_effective_from")
w_part = Window.partitionBy("plan").orderBy(F.col("plan_stddev_samp_val").desc_nulls_first())
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("days_since_effective_from_rank").desc())
w6 = Window.partitionBy("currency", "plan").orderBy(F.col("days_since_effective_from").desc_nulls_first())
w7 = Window.partitionBy("currency").orderBy(F.col("val_dense_rank").desc_nulls_first())
w8 = Window.partitionBy("currency").orderBy("plan_stddev_samp_val_cume_dist")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

valid = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-04-01").cast("date")) & (F.col("plan") == "enterprise"))
    .select(
        "*",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

enriched = (
    valid
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("days_since_effective_from_rank", F.rank().over(w3))
)

scored = (
    enriched
    .withColumn("plan_stddev_samp_val_cume_dist", F.cume_dist().over(w_part))
    .withColumn("days_since_effective_from_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("days_since_effective_from_percent_rank", F.percent_rank().over(w6))
    .withColumn("val_dense_rank_percent_rank", F.percent_rank().over(w7))
)

filtered = (
    scored
    .filter(F.col("val") < 67)
    .withColumn("days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank", (F.col("days_since_effective_from_rank_percent_rank") * F.col("days_since_effective_from_percent_rank")))
)

remaining_keys = (
    filtered
    .select("id", "plan")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") == "EUR")
        .select("id", "plan")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

scored_price_tiers = (
    reconciled
    .select(
        "id",
        "days_since_effective_from",
        "val_dense_rank",
        "val_dense_rank_percent_rank",
        "days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank",
        F.rank().over(w8).alias("plan_stddev_samp_val_cume_dist_rank"),
    )
)

result = (
    scored_price_tiers
    .withColumn("val_dense_rank_percent_rank_tier", F.when(F.col("val_dense_rank_percent_rank") >= 0.63, "LARGE").otherwise("SMALL"))
    .withColumn("days_since_effective_from_tier", F.when(F.col("days_since_effective_from") >= 78, "HIGH").otherwise("LOW"))
    .cube("val_dense_rank_percent_rank_tier", "days_since_effective_from_tier")
    .agg(
        F.percentile_approx("val_dense_rank", 0.95).alias("val_dense_rank_p95"),
        F.avg("days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank").alias("avg_days_since_effective_from_rank_percent_rank_times_days_since_effective_from_percent_rank"),
        F.percentile_approx("val_dense_rank", 0.9).alias("val_dense_rank_p90"),
        F.grouping("val_dense_rank_percent_rank_tier").alias("grouping_level"),
    )
)
