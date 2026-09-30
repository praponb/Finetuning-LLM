from pyspark.sql import functions as F

matched_keys = (
    df_promotions
    .select("id", "channel")
    .intersect(
        df_promotions
        .filter(F.col("discount_pct").between(0.31, 0.33))
        .select("id", "channel")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("discount_pct") >= 0.33, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        "promo_code",
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
    )
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.count_if(F.col("promo_code") == "VIP").alias("vip_events"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
    )
)
