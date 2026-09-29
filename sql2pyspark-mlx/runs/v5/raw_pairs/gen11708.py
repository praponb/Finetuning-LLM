from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("promo_code")

scored = (
    df_promotions
    .withColumn("discount_pct_avg_2_prior", F.avg("discount_pct").over(w))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w2))
    .withColumn("discount_pct_avg_3_prior", F.avg("discount_pct").over(w3))
    .withColumn("promo_code_max_discount_pct", F.max("discount_pct").over(w4))
)

result = (
    scored
    .cube(
        "channel",
        "promo_code",
        F.when(F.col("discount_pct") >= 0.3, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.sum("promo_code_max_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("promo_code_max_discount_pct_per_bundle"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_pct_avg_3_prior"))).alias("partner_total_discount_pct_avg_3_prior"),
    )
    .orderBy(F.col("promo_code").desc())
)
