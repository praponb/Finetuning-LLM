from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.orderBy("ends_on_plus_77d", "id").rowsBetween(-6, -1)
w_running = Window.orderBy("event_seq", "id")
w5 = Window.orderBy("ends_on_plus_77d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("ends_on", "id")

valid = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2024-04-18"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.concat_ws("-", "promo_code", "channel").alias("promo_code_channel_key"),
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
    )
)

with_windows = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w2))
)

filtered = (
    with_windows
    .filter((F.col("ends_on") > F.to_date(F.lit("2024-04-22"))) & (F.col("starts_on") >= "2023-09-01"))
    .select(
        "id",
        "ends_on",
        "event_seq",
        "discount_pct_cume_dist",
        F.date_add("ends_on", 77).alias("ends_on_plus_77d"),
    )
)

with_windows_promotions = (
    filtered
    .select(
        "*",
        F.avg("discount_pct_cume_dist").over(w3).alias("discount_pct_cume_dist_avg_6_prior"),
        F.lag("discount_pct_cume_dist", 2).over(w_running).alias("prev_discount_pct_cume_dist"),
        F.first("event_seq", ignorenulls=True).over(w5).alias("first_known_event_seq"),
        F.lead("event_seq", 2).over(w6).alias("next_event_seq"),
        F.ntile(10).over(w_running).alias("event_seq_bucket"),
    )
)

cleaned = (
    with_windows_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-07").cast("date")) & (F.col("prev_discount_pct_cume_dist") < 0.38))
    .withColumn("prev_discount_pct_cume_dist_rounded", F.round("prev_discount_pct_cume_dist", 0))
)

result = (
    cleaned
    .select(
        "id",
        "ends_on",
        "event_seq",
        "discount_pct_cume_dist",
        "discount_pct_cume_dist_avg_6_prior",
        "prev_discount_pct_cume_dist",
        "next_event_seq",
        "event_seq_bucket",
        "prev_discount_pct_cume_dist_rounded",
    )
    .orderBy(F.col("event_seq_bucket").asc_nulls_first())
)
