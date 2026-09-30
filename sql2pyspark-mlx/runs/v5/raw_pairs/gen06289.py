from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_rounded").desc(), "id")
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("discount_pct_rounded_bucket").desc_nulls_last(), "id")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("discount_pct_rounded", F.round("discount_pct", 2))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

enriched = (
    base
    .filter(F.col("ends_on").between("2023-08-01", "2025-04-01"))
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.ntile(5).over(w).alias("discount_pct_rounded_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

valid = (
    enriched
    .filter(F.col("channel").isNotNull())
    .withColumn("seq_no_times_discount_pct", (F.col("seq_no") * F.col("discount_pct")))
    .withColumn("seq_no_minus_discount_pct_rounded_bucket", (F.col("seq_no") - F.col("discount_pct_rounded_bucket")))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_promotions
    .withColumn("seq_no_minus_discount_pct_rounded_bucket_tier", F.when(F.col("seq_no_minus_discount_pct_rounded_bucket") >= 69, "LARGE").otherwise("SMALL"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["seq_no_minus_discount_pct_rounded_bucket_tier", "seq_no_tier"], ["seq_no_minus_discount_pct_rounded_bucket_tier"]],
        "seq_no_minus_discount_pct_rounded_bucket_tier",
        "seq_no_tier",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("seq_no_times_discount_pct"), 2).alias("avg_seq_no_times_discount_pct"),
        F.count("*").alias("row_count"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.grouping_id().alias("gid"),
    )
)
