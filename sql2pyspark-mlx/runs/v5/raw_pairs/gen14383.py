from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("promo_code")

metrics = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.min("discount_pct").over(w_hist).alias("promo_code_min_discount_pct"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cumulative_discount_pct") >= 1, "LARGE").otherwise("SMALL").alias("cumulative_discount_pct_tier"),
        "promo_code",
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
    )
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.max(F.when(F.col("promo_code") == "VIP", F.col("cumulative_discount_pct"))).alias("vip_max_cumulative_discount_pct"),
        F.max("promo_code_min_discount_pct").alias("max_promo_code_min_discount_pct"),
        F.round(F.avg("cumulative_discount_pct"), 2).alias("avg_cumulative_discount_pct"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
    )
)
