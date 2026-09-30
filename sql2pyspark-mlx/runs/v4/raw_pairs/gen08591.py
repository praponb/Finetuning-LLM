from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("promo_code", "channel")

scored = (
    df_promotions
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("running_discount_pct_2", F.sum("discount_pct").over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("discount_pct_share_2", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4))
)

result = (
    scored
    .groupBy(
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
        F.date_trunc("QUARTER", "starts_on").alias("quarter_start"),
        "promo_code",
    )
    .agg(
        F.percentile_approx("running_discount_pct", [0.25, 0.5, 0.75]).alias("running_discount_pct_quartiles"),
        F.round(F.avg("running_discount_pct"), 2).alias("avg_running_discount_pct"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("running_discount_pct_2"))).alias("mobile_avg_running_discount_pct_2"),
    )
)
