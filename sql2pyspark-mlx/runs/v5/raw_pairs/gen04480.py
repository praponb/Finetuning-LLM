from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-05-27").cast("date")) & (F.col("ends_on") <= "2025-10-01"))
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    cleaned
    .groupBy("promo_code")
    .pivot("channel", ["web", "store"])
    .agg(F.sum("discount_pct").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
