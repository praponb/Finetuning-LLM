from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())
w_part = Window.partitionBy("plan")
w_hist = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w_recent = Window.partitionBy("plan").orderBy(F.col("plan_count_days_since_effective_from").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("val_cume_dist").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

base = (
    exploded
    .filter(F.col("effective_from") > "2023-08-01")
    .select(
        "*",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

scored = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.max("val").over(w_part).alias("plan_max_val"),
        F.count("days_since_effective_from").over(w_part).alias("plan_count_days_since_effective_from"),
        F.rank().over(w_hist).alias("val_rank"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("plan_count_days_since_effective_from_cume_dist"),
        F.dense_rank().over(w5).alias("val_cume_dist_dense_rank"),
        F.round((F.col("plan_count_days_since_effective_from") / F.sum("plan_count_days_since_effective_from").over(w_part)), 4).alias("plan_count_days_since_effective_from_share"),
        F.round((F.col("plan_count_days_since_effective_from") / F.sum("plan_count_days_since_effective_from").over(w_part)), 4).alias("plan_count_days_since_effective_from_share_2"),
        F.max("val_rank").over(w_part).alias("plan_max_val_rank"),
    )
)

grouped = (
    metrics
    .groupBy(
        "plan",
        F.when(F.col("plan_count_days_since_effective_from") >= 17, "LARGE").otherwise("SMALL").alias("plan_count_days_since_effective_from_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("val_cume_dist_dense_rank", 0.5).alias("val_cume_dist_dense_rank_p50"),
        F.round(F.sum("plan_count_days_since_effective_from_share_2") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("plan_count_days_since_effective_from_share_2_per_enterprise"),
        F.size(F.collect_set("plan")).alias("num_plans_2"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
)

result = (
    grouped
    .select(
        "plan",
        "plan_count_days_since_effective_from_tier",
        "val_cume_dist_dense_rank_p50",
        "num_plans_2",
    )
)
