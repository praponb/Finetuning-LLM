from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("promo_code").isin(["VIP", "BUNDLE", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("channel").isin(["web", "partner"]), "Priority").otherwise("Other").alias("channel_group"),
    )
)

result = (
    base
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "BUNDLE"])
    .agg(F.max("discount_pct").alias("value_sum"))
    .select(
        "channel",
        F.col("SPRING10").alias("spring10"),
        F.col("BUNDLE").alias("bundle"),
    )
)
