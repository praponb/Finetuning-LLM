from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-07-03"))) & F.col("channel").isNotNull())
    .withColumn("days_since_ends_on", F.datediff(F.current_date(), "ends_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    base
    .groupBy("channel")
    .pivot("promo_code", ["WELCOME5", "VIP"])
    .agg(F.avg("discount_pct").alias("amount"))
)
