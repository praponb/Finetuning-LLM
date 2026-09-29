from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc())

ranked = (
    df_promotions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("discount_pct") >= 0.34, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.min("discount_pct").alias("min_discount_pct"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct")).otherwise(0)).alias("vip_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles_2"),
    )
    .filter(F.col("vip_discount_pct") < 0)
)
