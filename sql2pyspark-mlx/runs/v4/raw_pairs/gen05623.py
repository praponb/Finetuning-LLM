from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency", "plan")
w4 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w5 = Window.orderBy(F.col("plan_count_val").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

kept_keys = (
    lines
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.month("effective_from").isin([2, 3, 4]))
        .select("id", "plan")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.avg("val").over(w).alias("plan_avg_val"),
        F.stddev_samp("val").over(w2).alias("currency_stddev_samp_val"),
        F.sum("val").over(w2).alias("currency_sum_val"),
        F.count("val").over(w).alias("plan_count_val"),
        F.max("val").over(w3).alias("currency_max_val"),
    )
)

scored_price_tiers = (
    scored
    .select(
        "*",
        F.rank().over(w4).alias("val_rank"),
        F.cume_dist().over(w5).alias("plan_count_val_cume_dist"),
    )
)

stats = (
    scored_price_tiers
    .groupBy(
        F.when(F.col("plan_count_val") >= 17, "LARGE").otherwise("SMALL").alias("plan_count_val_tier"),
    )
    .agg(
        F.max(F.when(F.col("currency") == "USD", F.col("val"))).alias("usd_max_val"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.count("*").alias("row_count"),
        F.sum("plan_count_val_cume_dist").alias("total_plan_count_val_cume_dist"),
    )
    .filter(F.col("usd_max_val") >= 0)
)

result = (
    stats
    .select(
        "plan_count_val_tier",
        "usd_max_val",
        "row_count",
        "total_plan_count_val_cume_dist",
    )
)
