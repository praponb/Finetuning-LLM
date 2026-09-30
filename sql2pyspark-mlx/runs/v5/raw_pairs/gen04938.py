from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("last7_avg_discount_pct")
w3 = Window.partitionBy("channel").orderBy("last7_avg_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w5 = Window.partitionBy("channel", "promo_code").orderBy("event_seq", "id")
w6 = Window.partitionBy("promo_code", "channel").orderBy("first_known_last7_avg_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy(F.col("last7_max_discount_pct").desc(), "id")
w8 = Window.partitionBy("channel").orderBy(F.col("last7_max_discount_pct_bucket").desc_nulls_first(), "id")

scored = (
    df_promotions
    .filter(F.col("promo_code") == "BUNDLE")
    .select(
        "*",
        F.max("discount_pct").over(w).alias("last7_max_discount_pct"),
        F.sum("discount_pct").over(w).alias("last7_sum_discount_pct"),
        F.avg("discount_pct").over(w).alias("last7_avg_discount_pct"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.rank().over(w2).alias("last7_avg_discount_pct_rank"),
        F.first("last7_avg_discount_pct", ignorenulls=True).over(w3).alias("first_known_last7_avg_discount_pct"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

enriched_promotions = (
    ranked
    .select(
        "id",
        "channel",
        "promo_code",
        "last7_sum_discount_pct",
        "last7_avg_discount_pct_rank",
        "first_known_last7_avg_discount_pct",
        "rn",
        F.sum("last7_sum_discount_pct").over(w6).alias("running_last7_sum_discount_pct"),
        F.ntile(4).over(w7).alias("last7_max_discount_pct_bucket"),
        F.last("first_known_last7_avg_discount_pct", ignorenulls=True).over(w3).alias("last_known_first_known_last7_avg_discount_pct"),
    )
)

ranked_promotions = (
    enriched_promotions
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked_promotions
    .groupBy(
        "channel",
        F.when(F.col("first_known_last7_avg_discount_pct") >= 0.28, "PREMIUM").otherwise("STANDARD").alias("first_known_last7_avg_discount_pct_tier"),
    )
    .agg(
        F.count_if(F.col("promo_code") == "VIP").alias("vip_events"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
)
