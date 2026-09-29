from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(~F.col("promo_code").isin(["SPRING10", "VIP"]))
    .withColumn("days_starts_on_to_ends_on", F.datediff("ends_on", "starts_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .groupBy("promo_code")
    .pivot("channel", ["store", "partner"])
    .agg(
        F.max("days_starts_on_to_ends_on").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)
