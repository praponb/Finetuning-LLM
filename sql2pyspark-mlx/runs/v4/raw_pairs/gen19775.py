from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w_hist = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("avg_next_discount_pct").desc())

metrics = (
    df_promotions
    .select(
        "*",
        F.lead("discount_pct", 1).over(w).alias("next_discount_pct"),
        F.first("channel", ignorenulls=True).over(w_hist).alias("first_known_channel"),
    )
)

result = (
    metrics
    .groupBy("promo_code", "channel")
    .agg(
        F.round(F.avg("next_discount_pct"), 2).alias("avg_next_discount_pct"),
        F.percentile_approx("next_discount_pct", [0.25, 0.5, 0.75]).alias("next_discount_pct_quartiles"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_pct_per_mobile"),
    )
    .withColumn("avg_next_discount_pct_rank", F.rank().over(w3))
    .orderBy(F.col("promo_code").desc_nulls_last())
)
