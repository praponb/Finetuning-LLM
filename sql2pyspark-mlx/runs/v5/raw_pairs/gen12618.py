from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-05-04").cast("date"))
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 1))
)

result = (
    valid
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "VIP"])
    .agg(F.avg("discount_pct"))
)
