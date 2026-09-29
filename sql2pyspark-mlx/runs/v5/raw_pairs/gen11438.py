from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel").orderBy("ends_on", "id")
w_grp = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w4 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("prev_discount_pct", "id")
w_hist = Window.partitionBy("channel")
w7 = Window.partitionBy("channel").orderBy(F.col("first_known_prev_discount_pct").desc(), "id")
w8 = Window.partitionBy("channel").orderBy("first_known_prev_discount_pct", "id")
w9 = Window.partitionBy("channel").orderBy("first_known_prev_discount_pct_bucket", "id")
w10 = Window.partitionBy("channel").orderBy("channel_min_next_discount_pct", "id")

cleaned = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-12-10").cast("date"))
    .select("*", F.date_add("ends_on", 86).alias("ends_on_plus_86d"))
)

cleaned_promotions = (
    cleaned
    .filter(F.col("ends_on_plus_86d").between("2024-03-22", "2024-12-21"))
    .withColumn("starts_on_label", F.date_format("starts_on", "yyyy-MM-dd"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

metrics = (
    cleaned_promotions
    .select(
        "id",
        "channel",
        "discount_pct",
        "ends_on_plus_86d",
        "starts_on_label",
        F.lead("discount_pct").over(w_recent).alias("next_discount_pct"),
        F.lag("discount_pct").over(w_recent).alias("prev_discount_pct"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.last("channel", ignorenulls=True).over(w_grp).alias("last_known_channel"),
    )
)

enriched = (
    metrics
    .filter(F.col("next_discount_pct").between(0.23, 0.27))
    .select(
        "id",
        "channel",
        "ends_on_plus_86d",
        "starts_on_label",
        "next_discount_pct",
        "seq_no",
        "last_known_channel",
        F.ntile(10).over(w3).alias("discount_pct_bucket"),
        F.first("prev_discount_pct", ignorenulls=True).over(w4).alias("first_known_prev_discount_pct"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

scored = (
    enriched
    .withColumn("channel_min_next_discount_pct", F.min("next_discount_pct").over(w_hist))
    .withColumn("first_known_prev_discount_pct_bucket", F.ntile(4).over(w7))
)

enriched_promotions = (
    scored
    .select(
        "id",
        "channel",
        "next_discount_pct",
        "seq_no",
        "last_known_channel",
        "first_known_prev_discount_pct",
        "first_known_prev_discount_pct_bucket",
        F.lag("channel_min_next_discount_pct", 1).over(w8).alias("prev_channel_min_next_discount_pct"),
        F.lag("first_known_prev_discount_pct_bucket", 2, 0).over(w9).alias("prev_first_known_prev_discount_pct_bucket"),
        F.lag("discount_pct_bucket", 2).over(w10).alias("prev_discount_pct_bucket"),
    )
)

result = (
    enriched_promotions
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("prev_first_known_prev_discount_pct_bucket")).otherwise(0)).alias("partner_prev_first_known_prev_discount_pct_bucket"),
        F.percentile_approx("prev_discount_pct_bucket", 0.95).alias("prev_discount_pct_bucket_p95"),
        F.round(F.sum("prev_channel_min_next_discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("prev_channel_min_next_discount_pct_per_store"),
        F.max("next_discount_pct").alias("max_next_discount_pct"),
    )
    .orderBy(F.col("prev_discount_pct_bucket_p95").desc_nulls_last())
)
