from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-05-26").cast("date"))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.round((F.col("discount_pct") * 1.15), 2).alias("discount_pct_adjusted"),
        F.date_format("ends_on", "yyyy-MM-dd").alias("ends_on_label"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)
