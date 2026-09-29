from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency").orderBy(F.col("val_rank").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.rank().over(w).alias("val_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("plan") == "pro")
    .select(
        "plan",
        "effective_from",
        "currency",
        "val_rank",
        F.when(F.col("plan") == "pro", 1).otherwise(0).alias("is_pro"),
    )
)

scored = (
    valid
    .select(
        "effective_from",
        "val_rank",
        F.round((F.col("val_rank") / F.sum("val_rank").over(w2)), 4).alias("val_rank_share"),
        F.cume_dist().over(w3).alias("val_rank_cume_dist"),
        F.max("is_pro").over(w2).alias("plan_max_is_pro"),
    )
)

result = (
    scored
    .rollup(
        F.when(F.col("val_rank_share") >= 0.72, "LARGE").otherwise("SMALL").alias("val_rank_share_tier"),
        F.when(F.col("val_rank_cume_dist") >= 0.63, "PREMIUM").otherwise("STANDARD").alias("val_rank_cume_dist_tier"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("val_rank", [0.25, 0.5, 0.75]).alias("val_rank_quartiles"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.round(F.avg("val_rank_share"), 2).alias("avg_val_rank_share"),
        F.round(F.avg("val_rank"), 2).alias("avg_val_rank"),
        F.percentile_approx("val_rank_cume_dist", 0.9).alias("val_rank_cume_dist_p90"),
    )
    .filter(F.col("val_rank_cume_dist_p90") >= 0)
    .orderBy(F.col("val_rank_share_tier").desc_nulls_last())
)
