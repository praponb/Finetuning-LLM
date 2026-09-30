from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("plan_stddev_samp_val").desc())
w4 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w5 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc_nulls_last())
w6 = Window.orderBy("val_cume_dist")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

filtered = (
    flattened
    .filter(F.col("effective_from") > F.lit("2024-05-22").cast("date"))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("currency_lower", F.lower("currency"))
)

scored = filtered.select("*", F.stddev_samp("val").over(w).alias("plan_stddev_samp_val"))

enriched = (
    scored
    .select(
        "*",
        F.min("plan_stddev_samp_val").over(w2).alias("currency_min_plan_stddev_samp_val"),
        F.rank().over(w3).alias("plan_stddev_samp_val_rank"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
        F.cume_dist().over(w5).alias("val_cume_dist"),
    )
)

scored_price_tiers = (
    enriched
    .select(
        "id",
        "effective_from",
        "currency",
        "clean_plan",
        "plan_stddev_samp_val",
        "currency_min_plan_stddev_samp_val",
        "plan_stddev_samp_val_rank",
        F.rank().over(w6).alias("val_cume_dist_rank"),
        F.count("val_cume_dist").over(w).alias("plan_count_val_cume_dist"),
    )
)

scored_price_tiers_2 = (
    scored_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "clean_plan",
        "currency_min_plan_stddev_samp_val",
        "plan_stddev_samp_val_rank",
        "val_cume_dist_rank",
        "plan_count_val_cume_dist",
        F.avg("plan_stddev_samp_val").over(w2).alias("currency_avg_plan_stddev_samp_val"),
        F.round((F.col("val_cume_dist_rank") / F.sum("val_cume_dist_rank").over(w2)), 4).alias("val_cume_dist_rank_share"),
        F.max("currency_min_plan_stddev_samp_val").over(w2).alias("currency_max_currency_min_plan_stddev_samp_val"),
    )
)

valid = (
    scored_price_tiers_2
    .filter(F.col("effective_from") > F.lit("2024-02-12").cast("date"))
    .withColumn("least_currency_max_currency_min_plan_stddev_samp_val_val_cume_dist_rank_share", F.least("currency_max_currency_min_plan_stddev_samp_val", "val_cume_dist_rank_share"))
)

totals = (
    valid
    .groupBy("currency", F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.sum(F.when(F.col("currency") == "USD", F.col("currency_min_plan_stddev_samp_val")).otherwise(0)).alias("usd_currency_min_plan_stddev_samp_val"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
)

result = totals.select("currency", "week_start", "thb_count", "eur_rows")
