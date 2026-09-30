from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("discount_pct_tier").orderBy(F.col("discount_pct_per_web").desc())

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.32, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        "channel",
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("partner_count") >= 1)
    .withColumn("discount_pct_per_web_rank", F.rank().over(w))
)
