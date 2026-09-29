from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-07-31")))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code").isin(["BUNDLE", "SPRING10"]), "Group A").otherwise("Other").alias("promo_code_group"),
        F.date_sub("starts_on", 59).alias("starts_on_minus_59d"),
    )
)

cleaned = (
    valid
    .filter(F.col("discount_pct").between(0.16, 0.33))
    .select(
        "*",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.when(F.col("promo_code").isin(["VIP", "BUNDLE"]), "Group A").otherwise("Other").alias("promo_code_group_2"),
    )
)

cleaned_promotions = (
    cleaned
    .filter((F.col("ends_on") >= F.lit("2023-08-27").cast("date")) & (F.col("promo_code") == "WELCOME5"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_minus_59d",
        "ends_on_month_end",
        "promo_code_group_2",
        F.when(F.col("discount_pct") >= 0.35, "High").when(F.col("discount_pct") >= 0.15, "Medium").otherwise("Low").alias("discount_pct_tier"),
    )
)

valid_promotions = (
    cleaned_promotions
    .filter(F.col("starts_on") >= F.lit("2023-07-23").cast("date"))
    .withColumn("days_ends_on_to_ends_on_month_end", F.datediff("ends_on_month_end", "ends_on"))
)

result = (
    valid_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_minus_59d",
        "ends_on_month_end",
        "discount_pct_tier",
        "days_ends_on_to_ends_on_month_end",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
)
