from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank_2"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_2"),
    )
)

valid = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-10-25").cast("date"))
    .select(
        "*",
        F.round("tier_value_percent_rank_2", 0).alias("tier_value_percent_rank_2_rounded"),
        (F.col("tier_value_cume_dist_2") * F.col("tier_value_percent_rank_2")).alias("tier_value_cume_dist_2_times_tier_value_percent_rank_2"),
    )
)

base = (
    valid
    .filter(F.col("tier_value_percent_rank").between(0.23, 0.28))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value_percent_rank",
        "tier_value_cume_dist",
        F.col("tier_value_percent_rank_2_rounded").cast("int").alias("tier_value_percent_rank_2_rounded_int"),
    )
)

result = (
    base
    .select(
        "plan",
        "tier_value_cume_dist",
        F.when(F.col("plan").rlike("^(enterprise|pro)$"), "PRIORITY").when(F.col("plan").rlike("^(starter|basic)$"), "CORE").otherwise("EXTENDED").alias("plan_zone"),
    )
)
