from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

ranked = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.3, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "ends_on"))
    .groupingSets(
        [["discount_pct_tier", "quarter_start"], ["discount_pct_tier"]],
        "discount_pct_tier",
        "quarter_start",
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.min("discount_pct").alias("min_discount_pct"),
    )
)
