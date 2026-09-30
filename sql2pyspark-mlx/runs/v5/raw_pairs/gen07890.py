from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w_grp = Window.partitionBy("plan").orderBy(F.col("plan_sum_val").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy("val_dense_rank")
w5 = Window.partitionBy("plan").orderBy(F.col("plan_max_val_dense_rank").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "plan",
        "effective_from",
        F.sum("val").over(w).alias("plan_sum_val"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "id",
        "plan",
        "effective_from",
        "val_dense_rank",
        F.max("val_dense_rank").over(w).alias("plan_max_val_dense_rank"),
        F.dense_rank().over(w_grp).alias("plan_sum_val_dense_rank"),
        F.avg("val_dense_rank").over(w).alias("plan_avg_val_dense_rank"),
        F.dense_rank().over(w4).alias("val_dense_rank_dense_rank"),
    )
)

scored = (
    with_windows_price_tiers
    .filter(F.col("plan_sum_val_dense_rank") <= 3)
    .select(
        "*",
        F.round((F.col("plan_max_val_dense_rank") / F.sum("plan_max_val_dense_rank").over(w)), 4).alias("plan_max_val_dense_rank_share"),
        F.dense_rank().over(w5).alias("plan_max_val_dense_rank_dense_rank"),
        F.round((F.col("plan_max_val_dense_rank") / F.sum("plan_max_val_dense_rank").over(w)), 4).alias("plan_max_val_dense_rank_share_2"),
        F.count("val_dense_rank").over(w).alias("plan_count_val_dense_rank"),
    )
)

valid = (
    scored
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-05-04"))) & (F.col("plan_sum_val_dense_rank") <= 8))
    .select(
        "*",
        F.greatest("plan_max_val_dense_rank_share", "plan_count_val_dense_rank").alias("greatest_plan_max_val_dense_rank_share_plan_count_val_dense_rank"),
    )
)

filtered = (
    valid
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-03-20")))
    .select(
        "*",
        F.col("plan_sum_val_dense_rank").cast("string").alias("plan_sum_val_dense_rank_num"),
    )
)

valid_price_tiers = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-06-13").cast("date")) & F.col("plan_sum_val_dense_rank").between(6, 8))
    .withColumn("least_plan_count_val_dense_rank_val_dense_rank_dense_rank", F.least("plan_count_val_dense_rank", "val_dense_rank_dense_rank"))
)

result = (
    valid_price_tiers
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.round(F.sum("plan_max_val_dense_rank_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("plan_max_val_dense_rank_dense_rank_per_basic"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("plan") == "starter", F.col("plan_count_val_dense_rank")).otherwise(0)).alias("starter_plan_count_val_dense_rank"),
        F.percentile_approx("plan_max_val_dense_rank_share", [0.25, 0.5, 0.75]).alias("plan_max_val_dense_rank_share_quartiles"),
        F.sum(F.when(F.col("plan") == "starter", F.col("plan_max_val_dense_rank_dense_rank"))).alias("starter_total_plan_max_val_dense_rank_dense_rank"),
        F.avg(F.when(F.col("plan") == "basic", F.col("plan_max_val_dense_rank"))).alias("basic_avg_plan_max_val_dense_rank"),
    )
    .filter(F.col("starter_total_plan_max_val_dense_rank_dense_rank") < 1000000)
    .orderBy(F.col("starter_plan_count_val_dense_rank").desc_nulls_last())
)
