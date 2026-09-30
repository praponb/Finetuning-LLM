from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-10-13").cast("date")) & F.col("channel").isNotNull())
    .withColumn("starts_on_dayofweek", F.dayofweek("starts_on"))
)

result = (
    valid
    .groupBy("channel")
    .pivot("promo_code", ["WELCOME5", "SPRING10", "BUNDLE"])
    .agg(F.avg("discount_pct").alias("total"))
)
