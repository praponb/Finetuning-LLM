from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-10-24").cast("date"))
    .select("*", F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"))
)

result = (
    valid
    .groupBy("promo_code")
    .pivot("channel", ["mobile", "partner", "web"])
    .agg(F.sum("discount_pct").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
