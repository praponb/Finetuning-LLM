from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")

enriched = (
    df_promotions
    .withColumn("discount_pct_avg_6", F.avg("discount_pct").over(w))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w3))
)

result = (
    enriched
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .withColumn("next_discount_pct_tier", F.when(F.col("next_discount_pct") >= 0.33, "HIGH").otherwise("LOW"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.32, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "next_discount_pct_tier", "discount_pct_tier"], ["year_start", "next_discount_pct_tier"], ["year_start"], []],
        "year_start",
        "next_discount_pct_tier",
        "discount_pct_tier",
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct_avg_6")).otherwise(0)).alias("welcome5_discount_pct_avg_6"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.round(F.sum("next_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("next_discount_pct_per_vip"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct_avg_6")).otherwise(0)).alias("vip_discount_pct_avg_6"),
    )
    .filter(F.col("welcome5_discount_pct_avg_6") < 0)
)
