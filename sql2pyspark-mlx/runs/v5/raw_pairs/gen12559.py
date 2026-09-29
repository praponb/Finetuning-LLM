from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("discount_pct") >= 0.2)
    .select(
        "*",
        F.substring("channel", 1, 2).alias("channel_prefix"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    base
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
    )
    .filter(F.col("discount_pct_p50") > 0)
)
