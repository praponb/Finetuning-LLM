from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("days_starts_on_to_ends_on", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w4 = Window.partitionBy("channel")
w5 = Window.orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")
w7 = Window.partitionBy("channel", "promo_code").orderBy("days_starts_on_to_ends_on_bucket_avg_3", "id")
w8 = Window.orderBy(F.col("discount_pct").desc())

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("promo_code_channel_key", F.concat_ws("-", "promo_code", "channel"))
    .withColumn("days_starts_on_to_ends_on", F.datediff("ends_on", "starts_on"))
)

with_windows = (
    cleaned
    .withColumn("last_known_days_starts_on_to_ends_on", F.last("days_starts_on_to_ends_on", ignorenulls=True).over(w))
    .withColumn("days_starts_on_to_ends_on_bucket", F.ntile(3).over(w2))
)

with_windows_promotions = (
    with_windows
    .filter(F.col("discount_pct") > 0.21)
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
        F.avg("days_starts_on_to_ends_on_bucket").over(w5).alias("days_starts_on_to_ends_on_bucket_avg_3"),
    )
)

deduped = (
    with_windows_promotions
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_windows_promotions_2 = (
    deduped
    .withColumn("prev_discount_pct", F.lag("discount_pct").over(w7))
    .withColumn("next_discount_pct", F.lead("discount_pct").over(w7))
    .withColumn("prev_discount_pct_share", F.lag("discount_pct_share").over(w7))
    .withColumn("next_discount_pct_share", F.lead("discount_pct_share").over(w7))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w8))
)

result = (
    with_windows_promotions_2
    .select(
        "id",
        "promo_code",
        "last_known_days_starts_on_to_ends_on",
        "days_starts_on_to_ends_on_bucket",
        "seq_no",
        "discount_pct_share",
        "days_starts_on_to_ends_on_bucket_avg_3",
        "latest_rank",
        "prev_discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|VIP)$"), "CORE").when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code_zone").asc_nulls_first())
)
