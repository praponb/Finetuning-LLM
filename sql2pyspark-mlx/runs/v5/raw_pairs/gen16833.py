from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("channel").orderBy("discount_pct_bucket", "id")
w5 = Window.partitionBy("promo_code").orderBy("discount_pct_avg_5_prior", "id")
w_grp = Window.partitionBy("channel").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("promo_code") == "BUNDLE")
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("ends_on_plus_months", F.add_months("ends_on", 3))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

scored = (
    filtered
    .withColumn("discount_pct_bucket", F.ntile(10).over(w))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w2))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
)

enriched = (
    scored
    .filter(F.col("discount_pct").between(0.19, 0.24))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "channel_title",
        "ends_on_plus_months",
        "discount_pct_avg_5_prior",
        F.lag("discount_pct_bucket").over(w_part).alias("prev_discount_pct_bucket"),
        F.lag("discount_pct").over(w_part).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_part).alias("next_discount_pct"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.max("prev_discount_pct").over(w_grp).alias("running_max_prev_discount_pct"),
        F.sum("prev_discount_pct").over(w_grp).alias("running_sum_prev_discount_pct"),
        F.count("prev_discount_pct").over(w_grp).alias("running_count_prev_discount_pct"),
        F.min("prev_discount_pct").over(w_grp).alias("running_min_prev_discount_pct"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "channel_title",
        "ends_on_plus_months",
        "discount_pct_avg_5_prior",
        "prev_discount_pct_bucket",
        "prev_discount_pct",
    )
)
