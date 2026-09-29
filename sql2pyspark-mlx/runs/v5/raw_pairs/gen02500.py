from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan")
w_recent = Window.partitionBy("plan").orderBy(F.col("plan_length").asc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("plan_length").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_prices").alias("tier_value"),
    )
)

valid = (
    exploded
    .filter(F.col("effective_from").between("2023-05-01", "2025-06-01"))
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

kept_keys = (
    valid
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from").between("2024-09-21", "2025-04-05"))
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_price_tiers = (
    reconciled
    .filter(F.col("effective_from") >= F.lit("2024-03-24").cast("date"))
    .select("plan", F.length("plan").alias("plan_length"))
)

with_windows = (
    valid_price_tiers
    .select(
        "*",
        F.max("plan_length").over(w_grp).alias("plan_max_plan_length"),
        F.max("plan_length").over(w_grp).alias("plan_max_plan_length_2"),
        F.stddev_samp("plan_length").over(w_grp).alias("plan_stddev_samp_plan_length"),
        F.dense_rank().over(w_recent).alias("plan_length_dense_rank"),
        F.percent_rank().over(w3).alias("plan_length_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("plan_max_plan_length") >= 67, "LARGE").otherwise("SMALL").alias("plan_max_plan_length_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.percentile_approx("plan_length_percent_rank", [0.25, 0.5, 0.75]).alias("plan_length_percent_rank_quartiles"),
        F.sum(F.when(F.col("plan") == "pro", F.col("plan_length")).otherwise(0)).alias("pro_plan_length"),
    )
    .orderBy(F.col("basic_events").desc_nulls_first())
)
