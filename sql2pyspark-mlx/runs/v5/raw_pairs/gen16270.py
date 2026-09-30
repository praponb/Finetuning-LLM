from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("plan").orderBy(F.col("currency_min_val").desc_nulls_last())
w6 = Window.partitionBy("plan").orderBy("val_percent_rank_2")
w7 = Window.orderBy(F.col("val_percent_rank_2").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "plan",
        F.avg("val").over(w).alias("plan_avg_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.percent_rank().over(w3).alias("val_percent_rank_2"),
        F.min("val").over(w4).alias("currency_min_val"),
    )
)

scored = (
    metrics
    .filter(F.col("currency_min_val") != 62)
    .withColumn("currency_min_val_rank", F.rank().over(w5))
    .withColumn("plan_stddev_samp_val_percent_rank", F.stddev_samp("val_percent_rank").over(w))
    .withColumn("val_percent_rank_2_cume_dist", F.cume_dist().over(w6))
    .withColumn("val_percent_rank_2_percent_rank", F.percent_rank().over(w7))
    .withColumn("currency_min_val_rank_2", F.rank().over(w5))
)

filtered = (
    scored
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "plan_avg_val",
        "val_percent_rank_2",
        "currency_min_val",
        "plan_stddev_samp_val_percent_rank",
        "val_percent_rank_2_cume_dist",
        "val_percent_rank_2_percent_rank",
        "currency_min_val_rank_2",
        F.round((F.col("val_percent_rank") * 1.15), 2).alias("val_percent_rank_adjusted"),
    )
)

base = (
    filtered
    .filter(F.col("val_percent_rank_2_cume_dist") >= 0.73)
    .select(
        "*",
        (F.col("val_percent_rank_2") - F.col("currency_min_val_rank_2")).alias("val_percent_rank_2_minus_currency_min_val_rank_2"),
        F.round((F.col("plan_avg_val") * 1.15), 2).alias("adjusted_plan_avg_val"),
    )
)

grouped = (
    base
    .withColumn("val_percent_rank_2_percent_rank_tier", F.when(F.col("val_percent_rank_2_percent_rank") >= 0.68, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["val_percent_rank_2_percent_rank_tier", "plan"], ["val_percent_rank_2_percent_rank_tier"]],
        "val_percent_rank_2_percent_rank_tier",
        "plan",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("currency_min_val_rank_2").alias("max_currency_min_val_rank_2"),
        F.max(F.when(F.col("plan") == "pro", F.col("val_percent_rank_2_cume_dist"))).alias("pro_max_val_percent_rank_2_cume_dist"),
        F.percentile_approx("val_percent_rank_2_minus_currency_min_val_rank_2", [0.25, 0.5, 0.75]).alias("val_percent_rank_2_minus_currency_min_val_rank_2_quartiles"),
        F.round(F.avg("val_percent_rank_2_minus_currency_min_val_rank_2"), 2).alias("avg_val_percent_rank_2_minus_currency_min_val_rank_2"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    grouped
    .select(
        "plan",
        "max_currency_min_val_rank_2",
        "pro_max_val_percent_rank_2_cume_dist",
        "val_percent_rank_2_minus_currency_min_val_rank_2_quartiles",
        "avg_val_percent_rank_2_minus_currency_min_val_rank_2",
    )
    .orderBy(F.col("plan").desc_nulls_first())
)
