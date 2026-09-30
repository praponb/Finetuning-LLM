from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .rollup(
        "promo_code",
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
        F.when(F.col("discount_pct") >= 0.26, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .orderBy(F.col("quarter_start").desc())
)
