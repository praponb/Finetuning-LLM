from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency").orderBy("tier_value")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

base = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-06-05").cast("date"))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        F.last_day("effective_from").alias("effective_from_month_end"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("tier_value") >= 81, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
        F.when(F.col("tier_value_rank") >= 9, "LARGE").otherwise("SMALL").alias("tier_value_rank_tier"),
        "plan",
    )
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.percentile_approx("tier_value_cume_dist", 0.9).alias("tier_value_cume_dist_p90"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
)
