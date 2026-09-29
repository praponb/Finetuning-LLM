from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-12-23"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.substring("promo_code", 1, 4).alias("promo_code_prefix"),
    )
)

result = (
    base
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .groupingSets(
        [["discount_pct_tier", "year_start"], ["discount_pct_tier"]],
        "discount_pct_tier",
        "year_start",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct")).otherwise(0)).alias("spring10_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.max("discount_pct").alias("max_discount_pct"),
        F.size(F.collect_set("channel")).alias("num_channels_2"),
    )
)
