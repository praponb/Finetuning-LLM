from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_share").desc_nulls_last(), "id")
w5 = Window.partitionBy("promo_code").orderBy("discount_pct_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") <= "2023-03-01"))
    .select(
        "*",
        F.when(F.col("promo_code").isin(["WELCOME5", "BUNDLE"]), "Core").otherwise("Other").alias("promo_code_group"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

scored = (
    base
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w))
    .withColumn("discount_pct_avg_6", F.avg("discount_pct").over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .filter(F.col("starts_on") < "2025-02-08")
    .withColumn("last_known_discount_pct_avg_6", F.last("discount_pct_avg_6", ignorenulls=True).over(w5))
    .withColumn("cumulative_discount_pct_avg_6", F.sum("discount_pct_avg_6").over(w6))
)

totals = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .cube("activity_month")
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list_2"),
        F.round(F.sum("last_known_discount_pct_avg_6") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("last_known_discount_pct_avg_6_per_partner"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select(
        "promo_code_list",
        "promo_code_list_2",
        "welcome5_count",
        "num_promo_codes",
        "mobile_events",
        "grouping_id",
    )
    .orderBy(F.col("welcome5_count").asc_nulls_last())
)
