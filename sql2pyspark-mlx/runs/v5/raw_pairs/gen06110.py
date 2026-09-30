from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("ends_on_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("promo_code_length", "id")
w3 = Window.orderBy("discount_pct_whole", "id")
w4 = Window.orderBy("running_discount_pct_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_discount_pct_whole", "id")
w6 = Window.orderBy("promo_code_length_bucket", "id").rowsBetween(-5, -1)

base = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-07-01"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.length("promo_code").alias("promo_code_length"),
    )
)

filtered = (
    base
    .filter(F.col("channel").isNotNull() & F.col("discount_pct").between(0.2, 0.23))
    .select(
        "id",
        "ends_on",
        "ends_on_month_end",
        "promo_code_length",
        F.substring("channel", 1, 4).alias("channel_prefix"),
        F.ceil("discount_pct").alias("discount_pct_whole"),
    )
)

cleaned = (
    filtered
    .filter(F.col("promo_code_length") == 56)
    .select(
        "id",
        "ends_on_month_end",
        "promo_code_length",
        "channel_prefix",
        "discount_pct_whole",
        F.last_day("ends_on").alias("ends_on_month_end_2"),
    )
)

scored = (
    cleaned
    .filter(F.col("ends_on_month_end").between("2023-06-01", "2025-09-01"))
    .select(
        "id",
        "ends_on_month_end",
        "promo_code_length",
        "channel_prefix",
        "discount_pct_whole",
        F.sum("discount_pct_whole").over(w_ordered).alias("running_discount_pct_whole"),
        F.ntile(4).over(w2).alias("promo_code_length_bucket"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "ends_on_month_end",
        "promo_code_length",
        "discount_pct_whole",
        "running_discount_pct_whole",
        "promo_code_length_bucket",
        F.sum("promo_code_length_bucket").over(w_ordered).alias("cumulative_promo_code_length_bucket"),
        F.lead("discount_pct_whole", 1).over(w3).alias("next_discount_pct_whole"),
    )
)

enriched_promotions = (
    enriched
    .select(
        "id",
        "ends_on_month_end",
        "discount_pct_whole",
        "promo_code_length_bucket",
        F.ntile(4).over(w3).alias("discount_pct_whole_bucket"),
        F.sum("cumulative_promo_code_length_bucket").over(w4).alias("cumulative_cumulative_promo_code_length_bucket"),
        F.sum("discount_pct_whole").over(w_ordered).alias("running_discount_pct_whole_2"),
        F.lag("promo_code_length", 2).over(w5).alias("prev_promo_code_length"),
        F.avg("discount_pct_whole").over(w6).alias("discount_pct_whole_avg_5_prior"),
    )
)

result = (
    enriched_promotions
    .select(
        "id",
        "ends_on_month_end",
        "discount_pct_whole",
        "discount_pct_whole_bucket",
        "cumulative_cumulative_promo_code_length_bucket",
        "running_discount_pct_whole_2",
        "prev_promo_code_length",
    )
    .orderBy("discount_pct_whole_bucket", "ends_on_month_end")
)
