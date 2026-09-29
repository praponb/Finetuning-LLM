from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")

deduped = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.32, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["promo_code", "discount_pct_tier"], ["promo_code"]],
        "promo_code",
        "discount_pct_tier",
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct"))).alias("welcome5_total_discount_pct"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
        F.grouping("promo_code").alias("grouping_level"),
    )
    .filter(F.col("welcome5_total_discount_pct") > 0)
    .orderBy("discount_pct_tier")
)
