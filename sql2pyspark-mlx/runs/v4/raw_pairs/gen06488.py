from pyspark.sql import functions as F

result = (
    df_promotions
    .cube(
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
        F.when(F.col("discount_pct") >= 0.3, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
)
