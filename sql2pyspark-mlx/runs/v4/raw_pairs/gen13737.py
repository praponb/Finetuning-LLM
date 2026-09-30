from pyspark.sql import functions as F

result = (
    df_promotions
    .withColumn("week_start", F.date_trunc("WEEK", "ends_on"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "ends_on"))
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .groupingSets(
        [["week_start", "quarter_start", "year_start"], ["week_start", "quarter_start"]],
        "week_start",
        "quarter_start",
        "year_start",
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
    )
)
