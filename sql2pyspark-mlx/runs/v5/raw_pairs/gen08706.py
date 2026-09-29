from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("promo_code").orderBy("last7_count_last4_max_discount_pct", "id")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("promo_code_channel", F.concat("promo_code", F.lit(" - "), "channel"))
)

with_windows = (
    valid
    .filter(F.col("channel") == "web")
    .select(
        "*",
        F.min("discount_pct").over(w_recent).alias("last4_min_discount_pct"),
        F.sum("discount_pct").over(w_recent).alias("last4_sum_discount_pct"),
        F.avg("discount_pct").over(w_recent).alias("last4_avg_discount_pct"),
        F.max("discount_pct").over(w_recent).alias("last4_max_discount_pct"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "*",
        F.min("last4_max_discount_pct").over(w3).alias("last7_min_last4_max_discount_pct"),
        F.avg("last4_max_discount_pct").over(w3).alias("last7_avg_last4_max_discount_pct"),
        F.max("last4_max_discount_pct").over(w3).alias("last7_max_last4_max_discount_pct"),
        F.count("last4_max_discount_pct").over(w3).alias("last7_count_last4_max_discount_pct"),
    )
)

with_windows_promotions_2 = (
    with_windows_promotions
    .select(
        "*",
        F.lead("discount_pct").over(w_running).alias("next_discount_pct"),
        F.lag("discount_pct").over(w_running).alias("prev_discount_pct"),
        F.lag("last7_count_last4_max_discount_pct").over(w_running).alias("prev_last7_count_last4_max_discount_pct"),
        F.lead("last7_count_last4_max_discount_pct").over(w_running).alias("next_last7_count_last4_max_discount_pct"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

result = (
    with_windows_promotions_2
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "promo_code_channel",
        "last4_min_discount_pct",
        "last4_avg_discount_pct",
        "discount_pct_cume_dist",
        "last7_avg_last4_max_discount_pct",
        F.when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("last7_avg_last4_max_discount_pct").desc())
)
